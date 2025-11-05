<p style="text-align: center;">
  <img src="logo_OCR.jpg" alt="Logo Academy" width="100">
</p>

# README Construire et tester une infrastructure de données
## Étape 1 – Préparation, nettoyage et normalisation des données météo

Cette étape du projet vise à préparer et nettoyer des données météo brutes issues de plusieurs sources (Excel et JSON).
Le but est d’obtenir un jeu de données unifié, cohérent et prêt pour une utilisation ultérieure.
À la fin, nous obtenons :

"stations_all.json" — un fichier unique de métadonnées sur les stations météo. 

ET

"mongo_ready_measurements.json" — un fichier d’observations météo nettoyées, fusionnées et normalisées.
(Le nom “mongo” vient de l’étape suivante, mais ici aucune base n’est impliquée.)

###  Arborescence du projet
```
.
data/
├─ brut/
│  ├─ Data_Source1_011024-071024.json
│  ├─ Weather Underground - Ichtegem, BE.xlsx
│  ├─ Weather Underground - La Madeleine, FR.xlsx
│
│  ├─ brut_JSONL_bucket_S3/
│  │  ├─ greencoop_JSON_source.jsonl
│  │  ├─ Ichtegem_BE.jsonl
│  │  └─ la_madeleine.jsonl
│
│  ├─ brut_with_dates_and_times/
│  │  ├─ Weather Underground - Ichtegem, BE_with_date_time.xlsx
│  │  └─ Weather Underground - La Madeleine, FR_with_date_time.xlsx
│
│  └─ clean/
│     ├─ mongo_ready_measurements.json
│     └─ stations_all.json
└─ src/
    ├─ add_dates_batch.py
    ├─ generate_stations_all_from_s3.py
    └─ transform_to_mongo_json.py
```

## Environnement  

Version utilisée : Python 3.11

##  Installation de l’environnement
### Créer et activer l’environnement virtuel
```bash
python -m venv .venv #création dossier ".venv"
.venv\Scripts\Activate.ps1 #active le venv
```
### Installer les dépendances
```bash
pip install -r requirements.txt
```
### fichier requirements.txt

```
boto3==1.40.58
botocore==1.40.58
pandas==2.3.0
numpy==2.3.4
openpyxl==3.1.5
pytz==2025.2
````

# Étapes de transformation
### Enrichissement des fichiers Excel
(...troncature du contenu...)
