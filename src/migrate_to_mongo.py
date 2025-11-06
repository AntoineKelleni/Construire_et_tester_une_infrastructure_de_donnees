
"""
migrate_to_mongo.py
---------------------------------
Script tout-en-un qui :
- crée la base et les collections MongoDB (si absentes),
- crée les index (unicité stations.id et measurements.(id_station, dh_utc)),
- importe les données depuis 2 fichiers JSON (format JSON Array) :
    - stations_all.json -> collection "stations"
    - mongo_ready_measurements.json -> collection "measurements"
- calcule un rapport de qualité post-migration avec un TAUX D'ERREURS global,
- exporte un rapport JSON (par défaut: data/reports/mongo_quality_report.json)

Usage (exemples) :
  # variables d'environnement facultatives : MONGO_URI, DB_NAME
  python migrate_to_mongo.py \
      --stations "data/clean/stations_all.json" \
      --measurements "data/clean/mongo_ready_measurements.json" \
      --report "data/reports/mongo_quality_report.json"

Pré-requis :
  - MongoDB en marche (localhost:27017 par défaut)
  - paquets Python : pymongo, tqdm
"""

import argparse
import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterable, Tuple

from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from tqdm import tqdm


DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DEFAULT_DB_NAME = os.getenv("DB_NAME", "weather_db")

REQUIRED_MEAS_FIELDS = ["id_station", "dh_utc", "DateTime"]
BOUNDS = {
    "temperature": (-50, 60),    # °C
    "humidite": (0, 100),        # %
    "pression": (800, 1100),     # hPa approx
    "vent_moyen": (0, 200),      # km/h approx
    "vent_rafales": (0, 250)     # km/h approx
}


def load_json_array(path: str) -> Iterable[Dict[str, Any]]:
    """Charge un fichier JSON de type tableau [ {...}, {...}, ... ]."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Le fichier {path} n'est pas un JSON Array.")
    return data


def ensure_collections_and_indexes(db):
    """Crée les collections si besoin et applique les index."""
    if "stations" not in db.list_collection_names():
        db.create_collection("stations")
    if "measurements" not in db.list_collection_names():
        db.create_collection("measurements")

    # Index unicité stations.id
    db.stations.create_index([("id", ASCENDING)], unique=True, name="uniq_station_id")
    # Index composite unicité measurements.(id_station, dh_utc)
    db.measurements.create_index([("id_station", ASCENDING), ("dh_utc", ASCENDING)],
                                 unique=True, name="uniq_meas_station_dhutc")
    # Index secondaire utile
    db.measurements.create_index([("DateTime", ASCENDING)], name="idx_datetime")


def import_stations(db, stations_path: str) -> Tuple[int, int]:
    """Upsert des stations par 'id'. Retourne (nb_inserts, nb_updates estimés)."""
    stations = load_json_array(stations_path)
    ops = []
    for s in stations:
        if "id" not in s:
            # on ignore les stations sans id (ne devrait pas arriver si fichiers clean)
            continue
        ops.append(
            UpdateOne({"id": s["id"]}, {"$set": s}, upsert=True)
        )
    inserts = updates = 0
    if ops:
        res = db.stations.bulk_write(ops, ordered=False)
        # PyMongo ne donne pas le détail inserts vs updates facilement ici.
        # On estime via upserted_count; le reste est update.
        inserts = res.upserted_count or 0
        updates = (res.matched_count or 0)
    return inserts, updates


def import_measurements(db, meas_path: str, chunk_size: int = 2000) -> Tuple[int, int]:
    """Upsert des mesures par (id_station, dh_utc). Retourne (nb_inserts, nb_updates estimés)."""
    measurements = load_json_array(meas_path)

    inserts = 0
    updates = 0
    ops = []
    for m in tqdm(measurements, desc="Import measurements"):
        if "id_station" not in m or "dh_utc" not in m:
            # on ignore si clé composite incomplète
            continue
        filt = {"id_station": m["id_station"], "dh_utc": m["dh_utc"]}
        ops.append(UpdateOne(filt, {"$set": m}, upsert=True))

        if len(ops) >= chunk_size:
            try:
                res = db.measurements.bulk_write(ops, ordered=False)
                inserts += (res.upserted_count or 0)
                updates += (res.matched_count or 0)
            except BulkWriteError as bwe:
                # On compte quand même ce qu'on peut et on continue
                res = bwe.details
                inserts += res.get("nUpserted", 0)
                updates += res.get("nMatched", 0)
            finally:
                ops = []

    if ops:
        try:
            res = db.measurements.bulk_write(ops, ordered=False)
            inserts += (res.upserted_count or 0)
            updates += (res.matched_count or 0)
        except BulkWriteError as bwe:
            res = bwe.details
            inserts += res.get("nUpserted", 0)
            updates += res.get("nMatched", 0)

    return inserts, updates


def is_number(x):
    return isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))


def parse_dt(s):
    # Ex: "YYYY-MM-DD HH:MM:SS" (UTC pour dh_utc). Adapter si nécessaire.
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def quality_report(db, report_path: str) -> Dict[str, Any]:
    """Calcule un rapport de qualité et écrit un JSON."""
    total = db.measurements.estimated_document_count()
    st_total = db.stations.estimated_document_count()

    required = REQUIRED_MEAS_FIELDS
    null_counts = {f: 0 for f in required}
    field_counts = {f: 0 for f in required}
    out_of_range = {k: 0 for k in BOUNDS.keys()}
    time_order_errors = 0
    errors = 0
    duplicates = 0

    # référentiel station
    known_stations = {s["id"] for s in db.stations.find({}, {"id": 1})}
    with_station = 0

    seen = set()
    last_dt_by_station: Dict[str, datetime] = {}

    cur = db.measurements.find({}, {"_id": 0})
    for doc in cur:
        bad = False

        # champs requis + nulls
        for f in required:
            field_counts[f] += 1
            if f not in doc or doc[f] in (None, ""):
                null_counts[f] += 1
                bad = True

        # référentiel station
        if "id_station" in doc and doc.get("id_station") in known_stations:
            with_station += 1
        else:
            bad = True

        # doublons logiques
        if "id_station" in doc and "dh_utc" in doc and doc["id_station"] and doc["dh_utc"]:
            key = (doc["id_station"], doc["dh_utc"])
            if key in seen:
                duplicates += 1
                bad = True
            else:
                seen.add(key)

        # bornes de valeurs
        for k, (lo, hi) in BOUNDS.items():
            if k in doc and doc[k] is not None:
                v = doc[k]
                if not is_number(v):
                    bad = True
                else:
                    if (v < lo) or (v > hi):
                        out_of_range[k] += 1
                        bad = True

        # ordre temporel par station (grossier)
        if "id_station" in doc and "dh_utc" in doc and doc["id_station"] and doc["dh_utc"]:
            try:
                dt = parse_dt(doc["dh_utc"])
                last = last_dt_by_station.get(doc["id_station"])
                if last and dt < last:
                    time_order_errors += 1
                    bad = True
                last_dt_by_station[doc["id_station"]] = dt
            except Exception:
                bad = True

        if bad:
            errors += 1

    error_rate = (errors / total) if total else 0.0
    completeness = {
        f: 1 - (null_counts.get(f, 0) / field_counts.get(f, 1))
        for f in required
    }
    ref_coverage = (with_station / total) if total else 0.0

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "db": DEFAULT_DB_NAME,
        "totals": {
            "stations": st_total,
            "measurements": total
        },
        "errors": {
            "total_errors": errors,
            "error_rate": round(error_rate, 6),
            "duplicates": duplicates,
            "time_order_errors": time_order_errors,
            "out_of_range_counts": out_of_range,
            "null_counts_required_fields": null_counts,
        },
        "quality": {
            "completeness_required_fields": completeness,
            "referential_coverage": round(ref_coverage, 6)
        }
    }

    # écriture du rapport
    Path(os.path.dirname(report_path)).mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return report


def main():
    ap = argparse.ArgumentParser(description="Migration & contrôle qualité MongoDB")
    ap.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI, help="URI MongoDB (défaut: %(default)s)")
    ap.add_argument("--db", default=DEFAULT_DB_NAME, help="Nom de base (défaut: %(default)s)")
    ap.add_argument("--stations", required=True, help="Chemin du JSON Array des stations")
    ap.add_argument("--measurements", required=True, help="Chemin du JSON Array des mesures")
    ap.add_argument("--report", default="data/reports/mongo_quality_report.json", help="Chemin du rapport qualité JSON")
    args = ap.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    print(f"[i] Connexion: {args.mongo_uri}  DB={args.db}")
    ensure_collections_and_indexes(db)

    print(f"[i] Import stations: {args.stations}")
    st_ins, st_upd = import_stations(db, args.stations)
    print(f"[OK] Stations upsert: inserts={st_ins}, updates≈{st_upd}")

    print(f"[i] Import measurements: {args.measurements}")
    ms_ins, ms_upd = import_measurements(db, args.measurements)
    print(f"[OK] Measurements upsert: inserts={ms_ins}, updates≈{ms_upd}")

    print(f"[i] Contrôle qualité → {args.report}")
    rep = quality_report(db, args.report)
    print(json.dumps(rep, ensure_ascii=False, indent=2))
    print("[DONE] Migration + rapport qualité terminés.")


if __name__ == "__main__":
    main()
