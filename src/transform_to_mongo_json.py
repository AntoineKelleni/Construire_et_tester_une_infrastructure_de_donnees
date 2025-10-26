#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
transform_to_mongo_json.py (S3 version complète)
------------------------------------------------
- Lecture des JSONL/JSON array Airbyte sur S3
- Dépaquetage du champ _airbyte_data
- Explosion du champ 'hourly' InfoClimat → lignes
- Conversion unités WU: °F→°C, mph→km/h, inHg→hPa, in→mm
- Colonnes Date, DateTime (locale Europe/Paris), dh_utc (UTC)
- Résumés par fichier + global
- Export: ../data/clean/mongo_ready_measurements.json (JSON array)

Prérequis :
  pip install boto3 pandas numpy pytz
  aws configure
"""

import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

import boto3
import numpy as np
import pandas as pd
import pytz

# ===================== CONFIG =====================

AWS_REGION = "eu-north-1"
S3_INPUTS = [
    "s3://amzn-s3-mongodb-airbyte/brut-sources/JSON/GreenCoop_JSON_Source/2025_10_24_1761320432876_0.jsonl",
    "s3://amzn-s3-mongodb-airbyte/brut-sources/JSON/Ichtegem_BE/2025_10_24_1761343021500_0.jsonl",
    "s3://amzn-s3-mongodb-airbyte/brut-sources/JSON/la_madeleine/2025_10_24_1761343297084_0.jsonl",
]

STATION_FALLBACK = {
    "greencoop_json_source": "07015",
    "infoclimat": "07015",
    "ichtegem_be": "IICHTE19",
    "la_madeleine": "ILAMAD25",
    "ichtegem": "IICHTE19",
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = PROJECT_ROOT / "data" / "clean" / "mongo_ready_measurements.json"
TZ_LOCAL = pytz.timezone("Europe/Paris")

TARGET_COLS = [
    "id_station", "dh_utc", "Date", "DateTime",
    "temperature", "pression", "humidite",
    "point_de_rosee", "visibilite",
    "vent_moyen", "vent_rafales", "vent_direction",
    "pluie_1h", "pluie_3h",
    "neige_au_sol", "nebulosite", "temps_omm",
]

# ===================== UTILS =====================

def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

def parse_s3_uri(uri: str):
    assert uri.startswith("s3://"), f"URI invalide: {uri}"
    rest = uri[5:]
    b, k = rest.split("/", 1)
    return b, k

def read_json_s3(uri: str) -> pd.DataFrame:
    """Lit un objet S3 JSON array ou JSONL et dépaquette _airbyte_data si besoin."""
    b, k = parse_s3_uri(uri)
    obj = s3_client().get_object(Bucket=b, Key=k)
    text = obj["Body"].read().decode("utf-8", errors="replace").strip()

    if text.startswith("["):
        data = json.loads(text)
        df = pd.DataFrame(data)
        fmt = "JSON array"
    else:
        df = pd.read_json(StringIO(text), lines=True)
        fmt = "JSONL"

    print(f" {uri} ({fmt})")

    if "_airbyte_data" in df.columns:
        print("    _airbyte_data détecté → dépaquetage")
        df = pd.json_normalize(df["_airbyte_data"])

    return df

# Conversions & helpers
def safe_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    s = str(x).strip().replace(",", ".")
    s = "".join(ch for ch in s if ch.isdigit() or ch in ".-eE")
    if s == "": return None
    try: return float(s)
    except: return None

def safe_int(x): v = safe_float(x); return None if v is None else int(round(v))
def safe_str(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    s = str(x).strip()
    return None if s in ("", "nan", "None") else s

def f_to_c(v): x = safe_float(v); return None if x is None else (x - 32.0) * 5.0 / 9.0
def mph_to_kmh(v): x = safe_float(v); return None if x is None else x * 1.609344
def inhg_to_hpa(v): x = safe_float(v); return None if x is None else x * 33.8638866667
def inch_to_mm(v): x = safe_float(v); return None if x is None else x * 25.4

def iso_utc_str(x) -> Optional[str]:
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    if pd.isna(ts): return None
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def detect_vendor(uri: str, df: pd.DataFrame) -> str:
    low = uri.lower()
    cols = {c.lower() for c in df.columns}
    if "greencoop_json_source" in low or "infoclimat" in low or "hourly" in cols:
        return "infoclimat"
    if any(c in cols for c in ["dew point","pressure","precip. rate.","speed","gust","time","date"]):
        return "wu"
    return "wu"

def detect_station(uri: str) -> str:
    low = uri.lower()
    for key, sid in STATION_FALLBACK.items():
        if key in low:
            return sid
    return "UNKNOWN"

# ===================== EXPLOSION INFOCLIMAT =====================

def explode_infoclimat_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Explose un champ 'hourly' imbriqué en lignes (cas InfoClimat)."""
    hourly_col = next((c for c in df.columns if c.lower().endswith("hourly")), None)
    if not hourly_col or df.empty:
        return pd.DataFrame()
    root = df.iloc[0][hourly_col]
    if isinstance(root, str):
        try:
            root = json.loads(root)
        except Exception:
            return pd.DataFrame()
    if not isinstance(root, dict):
        return pd.DataFrame()
    rows = []
    for stid, mesures in root.items():
        if not isinstance(mesures, list):
            continue
        for m in mesures:
            if not isinstance(m, dict):
                continue
            rec = dict(m)
            rec.setdefault("id_station", stid)
            rows.append(rec)
    return pd.DataFrame(rows)

def explode_infoclimat_hourly_flat(df: pd.DataFrame) -> pd.DataFrame:
    """Cas des colonnes 'hourly.<station_id>'."""
    hourly_cols = [c for c in df.columns if c.startswith("hourly.")]
    if not hourly_cols or df.empty:
        return pd.DataFrame()
    first = df.iloc[0]
    rows = []
    for col in hourly_cols:
        stid = col.split(".", 1)[1]
        val = first[col]
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                continue
        if isinstance(val, list):
            for m in val:
                if isinstance(m, dict):
                    rec = dict(m)
                    rec.setdefault("id_station", stid)
                    rows.append(rec)
    return pd.DataFrame(rows)

# NORMALISATION

def normalize_infoclimat(df: pd.DataFrame, station_id: str) -> pd.DataFrame:
    df = df.copy()

    # id_station : on préserve ce qui existe déjà 
    if "id_station" in df.columns and df["id_station"].notna().any():
        df["id_station"] = df["id_station"].astype(str).str.strip().replace({"": None})
        df["id_station"] = df["id_station"].fillna(station_id)
    else:
        df["id_station"] = station_id

    #  dh_utc : normalisation / construction 
    if "dh_utc" in df.columns:
        df["dh_utc"] = df["dh_utc"].apply(iso_utc_str)
    else:
        # fallback: timestamp direct, ou datetime, ou couple Date+Time
        tc = next((c for c in df.columns if str(c).lower() in ("timestamp", "datetime", "time")), None)
        if tc is not None:
            df["dh_utc"] = df[tc].apply(iso_utc_str)
        elif {"Date", "Time"}.issubset(df.columns):
            df["dh_utc"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce", utc=True)\
                              .dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            df["dh_utc"] = None  # au pire

    #  DateTime & Date locales (Europe/Paris)
    dhdt = pd.to_datetime(df["dh_utc"], utc=True, errors="coerce")
    df["DateTime"] = dhdt.dt.tz_convert(TZ_LOCAL).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["Date"]     = dhdt.dt.tz_convert(TZ_LOCAL).dt.strftime("%Y-%m-%d")

    # Typages numériques / texte
    num_cols = [
        "temperature", "pression", "humidite", "point_de_rosee",
        "vent_moyen", "vent_rafales", "vent_direction",
        "pluie_1h", "pluie_3h", "neige_au_sol", "nebulosite"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].apply(safe_float)

    if "visibilite" in df.columns:
        df["visibilite"] = df["visibilite"].apply(safe_int)

    if "temps_omm" in df.columns:
        df["temps_omm"] = df["temps_omm"].apply(safe_str)

    return df

def normalize_wu(df: pd.DataFrame, station_id: str) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)
    if "Date" in df.columns and "Time" in df.columns:
        dh = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce", utc=True)
    elif "DateTime" in df.columns:
        dh = pd.to_datetime(df["DateTime"], errors="coerce", utc=True)
    else:
        dh = pd.to_datetime(None)
    df["dh_utc"] = dh.dt.strftime("%Y-%m-%d %H:%M:%S")

    df_dt = pd.to_datetime(df["dh_utc"], utc=True, errors="coerce")
    df["DateTime"] = df_dt.dt.tz_convert(TZ_LOCAL).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["Date"] = df_dt.dt.tz_convert(TZ_LOCAL).dt.strftime("%Y-%m-%d")

    df["temperature"]    = df.get("Temperature", pd.Series([None]*len(df))).apply(f_to_c)
    df["point_de_rosee"] = df.get("Dew Point",  pd.Series([None]*len(df))).apply(f_to_c)
    df["pression"]       = df.get("Pressure",   pd.Series([None]*len(df))).apply(inhg_to_hpa)
    df["humidite"]       = df.get("Humidity",   pd.Series([None]*len(df))).apply(safe_float)
    df["vent_moyen"]     = df.get("Speed",      pd.Series([None]*len(df))).apply(mph_to_kmh)
    df["vent_rafales"]   = df.get("Gust",       pd.Series([None]*len(df))).apply(mph_to_kmh)
    df["pluie_1h"]       = df.get("Precip. Rate.",  pd.Series([None]*len(df))).apply(inch_to_mm)
    df["pluie_3h"]       = df.get("Precip. Accum.", pd.Series([None]*len(df))).apply(inch_to_mm)
    df["visibilite"]     = None
    df["neige_au_sol"]   = None
    df["nebulosite"]     = None
    df["temps_omm"]      = None
    df["vent_direction"] = None
    df["id_station"] = station_id
    return df

# ===================== LOGS =====================

def summarize(label: str, df: pd.DataFrame):
    """Affiche Plage UTC, Temp. moy et le nombre d'occurrences par station."""
    n = len(df)
    print(f"{label}: {n} lignes")

    # Plage temporelle
    ts = pd.to_datetime(df["dh_utc"], utc=True, errors="coerce")
    tmin, tmax = ts.min(), ts.max()
    if pd.notna(tmin) and pd.notna(tmax):
        print(f"  Plage UTC       : {tmin.strftime('%Y-%m-%d %H:%M:%S')} → {tmax.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("  Plage UTC       : n/d")

    # Température moyenne
    tmean = pd.to_numeric(df.get("temperature"), errors="coerce").mean()
    print(f"  Temp. moy (°C)  : {tmean:.2f}" if pd.notna(tmean) else "  Temp. moy (°C)  : n/d")

    # Occurrences par station
    if "id_station" in df.columns:
        counts = (
            df["id_station"]
            .astype(str).str.strip().replace({"": None})
            .fillna("NA")
            .value_counts(dropna=False)
        )
        if len(counts):
            top_line = ", ".join([f"{k}:{v}" for k, v in counts.items()])
            print(f"  Stations        : {top_line}")
        else:
            print("  Stations        : n/d")
    else:
        print("  Stations        : n/d")


# ===================== MAIN =====================

def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    frames: List[pd.DataFrame] = []

    for uri in S3_INPUTS:
        df_raw = read_json_s3(uri)
        vendor = detect_vendor(uri, df_raw)
        station = detect_station(uri)
        

        # Explosion InfoClimat si besoin
        if vendor == "infoclimat":
            exploded = explode_infoclimat_hourly(df_raw)
            if exploded.empty:
                exploded = explode_infoclimat_hourly_flat(df_raw)
            if not exploded.empty:
                df_raw = exploded

        df_norm = normalize_infoclimat(df_raw, station) if vendor == "infoclimat" else normalize_wu(df_raw, station)

        for c in TARGET_COLS:
            if c not in df_norm.columns:
                df_norm[c] = None
        df_norm = df_norm[TARGET_COLS]

        summarize(uri.split("/")[-1], df_norm)
        frames.append(df_norm)

    if not frames:
        print("!!!!! Aucun fichier valide lu depuis S3.")
        return

    df_final = pd.concat(frames, ignore_index=True)

    before = len(df_final)
    df_final.drop_duplicates(subset=["id_station", "dh_utc"], inplace=True)
    after = len(df_final)

    # ---------------- RÉSUMÉ GLOBAL ----------------
    print("\n==================== RÉSUMÉ GLOBAL ====================")
    print(f"Lignes agrégées avant dédup : {before}")
    print(f"Lignes après dédup          : {after}")

    # Température moyenne globale
    temp_global = pd.to_numeric(df_final.get("temperature"), errors="coerce").mean()
    if pd.notna(temp_global):
        print(f"Temp. moyenne globale (°C) : {temp_global:.2f}")
    else:
        print("Temp. moyenne globale (°C) : n/d")

    # Occurrences par station (global)
    if "id_station" in df_final.columns and not df_final["id_station"].empty:
        global_counts = (
            df_final["id_station"]
            .astype(str).str.strip().replace({"": None})
            .fillna("NA")
            .value_counts(dropna=False)
        )
        if not global_counts.empty:
            stations_line = ", ".join([f"{k}:{int(v)}" for k, v in global_counts.items()])
            print(f"Stations (global)          : {stations_line}")
        else:
            print("Stations (global)          : n/d")
    else:
        print("Stations (global)          : n/d")

    # Écriture du fichier final
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(df_final.to_json(orient="records", force_ascii=False))
    OUT_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nFichier MongoDB prêt écrit : {OUT_PATH} ({after} enregistrements)")

if __name__ == "__main__":
    main()
