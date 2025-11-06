import os, time, csv, statistics
from datetime import datetime
from pymongo import MongoClient

HOST = os.getenv("MONGO_HOST", "56.228.6.19")      
USER = os.getenv("MONGO_USER", "admin")
PWD  = os.getenv("MONGO_PASS", "MonSuperMotDePasse!")
DB   = os.getenv("MONGO_DB", "weather_db")
COL  = os.getenv("MONGO_COL", "measurements")

# Exemple de filtre
DATE_LOCAL = os.getenv("DATE_LOCAL", "2024-10-07")
STATION_ID = os.getenv("STATION_ID", "ILAMAD25")

RUNS = int(os.getenv("RUNS", "15"))               # nombre d’itérations
WARMUP = int(os.getenv("WARMUP", "3"))            # itérations d’échauffement (non comptées)
LIMIT = int(os.getenv("LIMIT", "0"))              # 0 = pas de limite

uri = f"mongodb://{USER}:{PWD}@{HOST}:27017/admin"
client = MongoClient(uri, serverSelectionTimeoutMS=5000)
coll = client[DB][COL]

# index pour accélérer la requête
try:
    coll.create_index([("Date", 1), ("id_station", 1), ("dh_utc", 1)], background=True)
except Exception:
    pass

query = {"Date": DATE_LOCAL, "id_station": STATION_ID}
projection = None  # ex: {"_id": 0, "temp_c": 1} si tu veux réduire la charge

def timed_find():
    t0 = time.perf_counter()
    cursor = coll.find(query, projection=projection)
    if LIMIT > 0:
        docs = list(cursor.limit(LIMIT))
    else:
        docs = list(cursor)
    dt_ms = (time.perf_counter() - t0) * 1000
    return dt_ms, len(docs)

# Warmup (remplit caches réseau/serveur)
for i in range(WARMUP):
    dt, n = timed_find()

# Mesures
runs = []
counts = []
for i in range(RUNS):
    dt, n = timed_find()
    runs.append(dt); counts.append(n)
    print(f"[{i+1}/{RUNS}] {n} docs en {dt:.1f} ms")

avg = statistics.mean(runs)
p50 = statistics.median(runs)
p95 = statistics.quantiles(runs, n=100)[94] if len(runs) >= 20 else max(runs)
mn, mx = min(runs), max(runs)

print("\n=== RÉSUMÉ LATENCE ===")
print(f"Docs (dernière requête) : {counts[-1] if counts else 0}")
print(f"moyenne: {avg:.1f} ms | médiane: {p50:.1f} ms | p95: {p95:.1f} ms | min: {mn:.1f} ms | max: {mx:.1f} ms")

# Sauvegarde CSV
stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
csv_path = f"latency_report_{stamp}.csv"
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["run", "ms"])
    for i, v in enumerate(runs, 1):
        w.writerow([i, f"{v:.3f}"])
print(f"Fichier écrit : {csv_path}")
