#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
check_data_integrity.py
-----------------------
Contrôles d'intégrité AVANT / APRÈS migration :

- Avant : fichier data/clean/mongo_ready_measurements.json
- Après : collection MongoDB weather_db.measurements

Vérifie :
- colonnes disponibles
- types (pandas)
- valeurs manquantes
- doublons sur la clé métier (id_station + dh_utc)
- comparaison des volumes avant / après
"""

import os
import json
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "data" / "clean" / "mongo_ready_measurements.json"

# Mongo local (sans authentification)
HOST = os.getenv("MONGO_HOST", "localhost")
DB   = os.getenv("MONGO_DB", "weather_db")
COL  = os.getenv("MONGO_COL", "measurements")

KEY_COLUMNS = ["id_station", "dh_utc"]  # clé logique d'une mesure



# ----------- LOADERS -----------

def load_source_df() -> pd.DataFrame:
    """Charge le JSON propre généré avant migration."""
    if not SRC_PATH.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {SRC_PATH}")
    data = json.loads(SRC_PATH.read_text(encoding="utf-8"))
    return pd.DataFrame(data)


def load_mongo_df() -> pd.DataFrame:
    """Charge les données depuis MongoDB (APRÈS migration), sans authentification."""
    uri = f"mongodb://{HOST}:27017/"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    try:
        coll = client[DB][COL]
        docs = list(coll.find({}, {"_id": 0}))
    except ServerSelectionTimeoutError as e:
        print(f"\n[AVERTISSEMENT] Impossible de joindre MongoDB ({uri}) : {e}")
        print("→ Vérifie que le conteneur / l’instance MongoDB est bien démarré.")
        return pd.DataFrame()

    if not docs:
        print(f"\n[INFO] Aucune donnée trouvée dans {DB}.{COL}")
        return pd.DataFrame()

    return pd.DataFrame(docs)



# ----------- PROFILAGE -----------

def profile_df(df: pd.DataFrame, label: str):
    """Affiche un profil de base : lignes, colonnes, types, NA, doublons."""
    print(f"\n===== PROFIL {label} =====")
    print(f"Lignes : {len(df)}")
    print(f"Colonnes : {list(df.columns)}")

    # types pandas
    print("\nTypes de colonnes :")
    print(df.dtypes)

    # valeurs manquantes
    print("\nValeurs manquantes (nb et %) :")
    na_counts = df.isna().sum()
    na_percent = (na_counts / len(df) * 100).round(1)
    for col in df.columns:
        print(f"  - {col:15s} : {na_counts[col]:5d} manquants ({na_percent[col]:4.1f} %)")

    # doublons sur la clé logique
    if all(c in df.columns for c in KEY_COLUMNS):
        dup = df.duplicated(subset=KEY_COLUMNS).sum()
        print(f"\nDoublons sur {KEY_COLUMNS} : {dup}")
    else:
        print(f"\nDoublons : impossible de vérifier, colonnes manquantes parmi {KEY_COLUMNS}")


def compare_schemas(df_src: pd.DataFrame, df_mongo: pd.DataFrame):
    """Compare colonnes + volumes entre AVANT et APRÈS migration."""
    print("\n===== COMPARAISON AVANT / APRÈS =====")
    src_cols = set(df_src.columns)
    mongo_cols = set(df_mongo.columns)

    only_src = sorted(src_cols - mongo_cols)
    only_mongo = sorted(mongo_cols - src_cols)
    common = sorted(src_cols & mongo_cols)

    print(f"Colonnes communes ({len(common)}) : {common}")
    if only_src:
        print(f"Colonnes uniquement dans le fichier source : {only_src}")
    if only_mongo:
        print(f"Colonnes uniquement dans MongoDB : {only_mongo}")

    print(f"\nLignes source : {len(df_src)}")
    print(f"Lignes MongoDB : {len(df_mongo)}")


# ----------- MAIN -----------

def main():
    # Avant migration : fichier JSON propre
    df_src = load_source_df()
    profile_df(df_src, "AVANT MIGRATION (fichier mongo_ready_measurements.json)")

    # Après migration : collection MongoDB
    df_mongo = load_mongo_df()
    if df_mongo.empty:
        print("\n[INFO] Profil APRÈS migration non disponible (Mongo vide ou injoignable).")
        return

    profile_df(df_mongo, "APRÈS MIGRATION (MongoDB weather_db.measurements)")

    # Comparaison globale
    compare_schemas(df_src, df_mongo)


if __name__ == "__main__":
    main()
