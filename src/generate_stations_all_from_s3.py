import os
import json
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# --- REGION & S3 OBJECTS (adapter si besoin) ---
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

S3_JSONL_URIS = [
    "s3://amzn-s3-mongodb-airbyte/brut-sources/JSON/GreenCoop_JSON_Source/2025_10_24_1761291533951_0.jsonl",
    "s3://amzn-s3-mongodb-airbyte/brut-sources/JSON/Ichtegem_BE/2025_10_24_1761291534283_0.jsonl",
    "s3://amzn-s3-mongodb-airbyte/brut-sources/JSON/la_madeleine_FR/2025_10_24_1761291535062_0.jsonl",
]


# --- Chemin de sortie du JSON final ---
BASE_DIR = Path(__file__).resolve().parent
OUT_PATH = (BASE_DIR / ".." / "data" / "clean" / "stations_all.json").resolve()

# --- 4 stations InfoClimat (exactement comme ton JSON initial) ---
INFOCLIMAT_STATIONS = [
    {
        "id": "00052",
        "name": "Armentières",
        "latitude": 50.689,
        "longitude": 2.877,
        "elevation": 16,
        "type": "static",
        "license": {
            "license": "CC BY",
            "url": "https://creativecommons.org/licenses/by/2.0/fr/",
            "source": "infoclimat.fr",
            "metadonnees": "https://www.infoclimat.fr/stations/metadonnees.php?id=00052"
        }
    },
    {
        "id": "000R5",
        "name": "Bergues",
        "latitude": 50.968,
        "longitude": 2.441,
        "elevation": 17,
        "type": "static",
        "license": {
            "license": "CC BY",
            "url": "https://creativecommons.org/licenses/by/2.0/fr/",
            "source": "infoclimat.fr",
            "metadonnees": "https://www.infoclimat.fr/stations/metadonnees.php?id=000R5"
        }
    },
    {
        "id": "07015",
        "name": "Lille-Lesquin",
        "latitude": 50.575,
        "longitude": 3.092,
        "elevation": 47,
        "type": "synop",
        "license": {
            "license": "Etalab Open License",
            "url": "https://www.etalab.gouv.fr/licence-ouverte-open-licence",
            "source": "Meteo-France via infoclimat.fr",
            "metadonnees": "https://donneespubliques.meteofrance.fr/metadonnees_publiques/fiches/fiche_59343001.pdf"
        }
    },
    {
        "id": "STATIC0010",
        "name": "Hazebrouck",
        "latitude": 50.734,
        "longitude": 2.545,
        "elevation": 31,
        "type": "static",
        "license": {
            "license": "CC BY",
            "url": "https://creativecommons.org/licenses/by/2.0/fr/",
            "source": "infoclimat.fr",
            "metadonnees": "https://www.infoclimat.fr/stations/metadonnees.php?id=STATIC0010"
        }
    }
]

# --- 2 stations WU (champs matériel/logiciel inconnus ici) ---
WU_STATIONS = [
    {
        "id": "ILAMAD25",
        "name": "La Madeleine",
        "latitude": 50.659,
        "longitude": 3.07,
        "elevation": 23,
        "type": "amateur",
        "license": None
    },
    {
        "id": "IICHTE19",
        "name": "WeerstationBS",
        "latitude": 51.092,
        "longitude": 2.999,
        "elevation": 15,
        "type": "amateur",
        "license": None
    }
]

def parse_s3_uri(uri: str):
    assert uri.startswith("s3://"), f"URI invalide: {uri}"
    rest = uri[5:]
    bucket, key = rest.split("/", 1)
    return bucket, key

def check_s3_objects_exist(uris):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    for uri in uris:
        b, k = parse_s3_uri(uri)
        try:
            s3.head_object(Bucket=b, Key=k)
            print(f"[OK] trouvé : {uri}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "404":
                print(f"[WARN] objet absent : {uri}")
            else:
                print(f"[WARN] impossible de vérifier {uri} ({code})")

def main():
    print("[i] Vérification de l'accessibilité des 3 JSONL S3…")
    check_s3_objects_exist(S3_JSONL_URIS)

    # Concatène les 6 stations au schéma d'origine
    stations = INFOCLIMAT_STATIONS + WU_STATIONS

    # Écrit le JSON (array) avec les mêmes clés que le JSON initial
    OUT_PATH.write_text(json.dumps(stations, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n|||OK|||| Fichier généré : {OUT_PATH}")
    print(f"   Nombre total de stations : {len(stations)}")
    print("   (4 InfoClimat + 2 Weather Underground)")

if __name__ == "__main__":
    main()
