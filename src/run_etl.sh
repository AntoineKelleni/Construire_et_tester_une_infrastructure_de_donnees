#!/usr/bin/env bash
set -euo pipefail

# ==== Chemins ====
SRC="/app/src"
DATA="/app/data"
CLEAN="${DATA}/clean"
REPORTS="${DATA}/reports"
mkdir -p "$CLEAN" "$REPORTS"

# ==== Vérif env ====
require_env() { : "${!1:?[ENV] $1 est requis}"; }
require_env S3_BUCKET
require_env S3_PREFIX
require_env AWS_DEFAULT_REGION
require_env MONGO_ROOT_USER
require_env MONGO_ROOT_PASS
require_env MONGO_DB

MONGO_HOST="${MONGO_HOST:-mongodb}"
MONGO_PORT="${MONGO_PORT:-27017}"

echo "[1/3] Génération du stations_all.json depuis S3…"
python "${SRC}/generate_stations_all_from_s3.py" \
  --bucket "${S3_BUCKET}" \
  --prefix "${S3_PREFIX}" \
  --region "${AWS_DEFAULT_REGION}" \
  --out "${CLEAN}/stations_all.json"

echo "[2/3] Transformation des mesures -> mongo_ready_measurements.json…"
python "${SRC}/transform_to_mongo_json.py" \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}" \
  --region "${AWS_DEFAULT_REGION}" \
  --out "${CLEAN}/mongo_ready_measurements.json" \
  --prefix-map "${STATION_PREFIX_MAP:-}"

echo "[3/3] Migration vers MongoDB + rapport qualité…"
python "${SRC}/migrate_to_mongo.py" \
  --stations "${CLEAN}/stations_all.json" \
  --measurements "${CLEAN}/mongo_ready_measurements.json" \
  --mongo-uri "mongodb://${MONGO_ROOT_USER}:${MONGO_ROOT_PASS}@${MONGO_HOST}:${MONGO_PORT}" \
  --db "${MONGO_DB}" \
  --report "${REPORTS}/mongo_quality_report.json"

echo " Terminé. Fichiers :"
echo " - ${CLEAN}/stations_all.json"
echo " - ${CLEAN}/mongo_ready_measurements.json"
echo " - ${REPORTS}/mongo_quality_report.json"
