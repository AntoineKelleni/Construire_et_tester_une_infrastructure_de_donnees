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


> Script : src/add_dates_batch.py  


* Objectif 

  Ajouter automatiquement les colonnes Date et DateTime dans chaque onglet Excel, à partir du nom de l’onglet et de la colonne Time.

* Principe

  Lit les fichiers dans data/brut/.

  Déduit la date depuis le nom de l’onglet (071024, 2024-10-07, etc.).

  Crée Date et DateTime (fusion de Date + Time).

  Sauvegarde dans data/brut_with_dates_and_times/.


Exemple d’entrée (Excel)
```
Time	TempOut	Humidity
00:00	14.8	91
01:00	14.2	93

Nom d’onglet : 071024
```
Exemple de sortie (Excel enrichi)

```
Date	    Time	DateTime	      TempOut	Humidity
2024-10-07	00:00	2024-10-07 00:00	14.8	91
2024-10-07	01:00	2024-10-07 01:00	14.2	93
```
Commande PowerShell

```
python src\add_dates_batch.py
```

### Génération du référentiel des stations

> Script : src/generate_stations_all_from_s3.py

* Objectif

  Créer un fichier unique stations_all.json regroupant toutes les métadonnées de stations (InfoClimat + Weather Underground).

Exemple de sortie (data/clean/stations_all.json)
```
[
  {
    "id_station": "ILAMAD25",
    "name": "La Madeleine",
    "latitude": 50.659,
    "longitude": 3.07,
    "elevation": 23,
    "hardware": "other",
    "software": "EasyWeatherPro_V5.1.6",
    "type": "amateur"
  },
  {
    "id_station": "IICHTE19",
    "name": "WeerstationBS",
    "latitude": 51.092,
    "longitude": 2.999,
    "elevation": 15,
    "hardware": "other",
    "software": "EasyWeatherV1.6.6",
    "type": "amateur"
  }
]
```
Commande PowerShell
```
python src\generate_stations_all_from_s3.py
```
### Dictionnaire de données – Base MongoDB weather_db
### Collection stations

Cette collection contient les métadonnées descriptives des stations météorologiques.
Chaque document représente une station unique, avec ses coordonnées géographiques et ses informations de licence.

'''
Champ	Type	Description
_id	ObjectId	Identifiant unique généré par MongoDB
id	String	Identifiant officiel de la station (ex. : "00052")
name	String	Nom de la station (ex. : "Armentières")
latitude	Float	Latitude géographique de la station
longitude	Float	Longitude géographique de la station
elevation	Integer	Altitude de la station en mètres
type	String	Type de station (ex. : "static")
source	String	Source des données (ex. : "infoclimat.fr")
metadonnees	String	URL vers les métadonnées de la station
license	Objet	Détail de la licence d’utilisation
license.license	String	Type de licence (ex. : "CC BY")
license.url	String	Lien vers le texte de la licence
'''

### Collection measurements

Cette collection contient les mesures météorologiques horodatées collectées par les stations.
Chaque document correspond à une observation météo pour une station donnée et une heure précise.

'''
Champ	Type	Description
_id	ObjectId	Identifiant unique MongoDB
id_station	String	Identifiant de la station émettrice de la mesure
dh_utc	String	Date/heure UTC au format ISO
Date	String	Date locale de la mesure (AAAA-MM-JJ)
DateTime	String	Date et heure locale (Europe/Paris)
temperature	Float	Température en °C
pression	Float	Pression atmosphérique en hPa
humidite	Float	Humidité relative en %
point_de_rosee	Float	Température du point de rosée en °C
visibilite	Float	Visibilité horizontale en mètres
vent_moyen	Float	Vitesse moyenne du vent en m/s
vent_rafales	Float	Rafales maximales du vent en m/s
vent_direction	Integer	Direction du vent en degrés (0–360°)
pluie_1h	Float	Cumul de pluie sur 1 heure (mm)
pluie_3h	Float	Cumul de pluie sur 3 heures (mm)
neige_au_sol	Float	Épaisseur de neige au sol (cm)
nebulosite	Float	Couverture nuageuse en %
temps_omm	String	Code OMM du temps observé
'''

### Lien entre les collections

measurements.id_station ↔ stations.id
→ Permet de relier chaque mesure à la station correspondante.

Schéma logique : 1 station → plusieurs mesures


### Normalisation et agrégation des mesures

> Script : src/transform_to_mongo_json.py

* Objectif

  Nettoyer, uniformiser et fusionner les mesures météo issues des sources JSONL.

* Traitements effectués

  Lecture des fichiers JSONL ( exportés d’un bucket AWS S3).

* Détection du fournisseur (WU ou InfoClimat).

```
Conversion des unités :

°F → °C

mph → km/h

inHg → hPa

in → mm
```

Création des colonnes :
```
dh_utc (horodatage UTC)

DateTime (Europe/Paris)

Date
```

Exemple d’entrée (extrait JSONL) S3 du fichier data\brut_JSONL_bucket_S3\greencoop_JSON_source.jsonl

```bash
{"id_station":"07015","dh_utc":"2024-10-05 16:00:00","temperature":"14.9","pression":"1014.5","humidite":"61","point_de_rosee":"7.4","visibilite":"19000","vent_moyen":"14.4","vent_rafales":"21.6","vent_direction":"100","pluie_3h":null,"pluie_1h":"0","neige_au_sol":null,"nebulosite":"","temps_omm":null}
```
Exemple de sortie normalisée
```bash
{
    "id_station": "07015",
    "dh_utc": "2024-10-05 00:00:00",
    "Date": "2024-10-05",
    "DateTime": "2024-10-05 02:00:00",
    "temperature": 7.6,
    "pression": 1020.7,
    "humidite": 89.0,
    "point_de_rosee": 5.9,
    "visibilite": 6000.0,
    "vent_moyen": 3.6,
    "vent_rafales": 7.2,
    "vent_direction": 90.0,
    "pluie_1h": 0.0,
    "pluie_3h": 0.0,
    "neige_au_sol": null,
    "nebulosite": null,
    "temps_omm": null
  },
```

Commande PowerShell
```
python src\transform_to_mongo_json.py
```


### Checklist de validation
      1	Excel enrichis dans data/brut_with_dates_and_times/	
      2	Fichier stations_all.json créé	
      3	Fichier mongo_ready_measurements.json généré	
      4	Résumé console sans erreur	
      5	Unités cohérentes (°C, km/h, hPa, mm)	

## Étape 2 – Migration MongoDB — Script tout-en-un

Le script Python "src\migrate_to_mongo.py" qui **crée la base**, **importe les données** et **mesure la qualité post-migration** (taux d’erreurs).

##  Fichiers
- `migrate_to_mongo.py` — script principal
- `flowchart_migration.mmd` — logigramme Mermaid (collez-le dans votre README GitHub pour rendu automatique)

##  Prérequis
- MongoDB local en cours d’exécution (`mongodb://localhost:27017` par défaut)
- Un venv activé avec :
  ```bash
  pip install -r requirements.txt
  ```

## Exécution
Depuis la racine du projet :
```bash
python migrate_to_mongo.py   --stations "data/clean/stations_all.json"   --measurements "data/clean/mongo_ready_measurements.json"   --report "data/reports/mongo_quality_report.json"
```

Variables d’environnement possibles :
```bash
set MONGO_URI="mongodb://localhost:27017"
set DB_NAME="weather_db"
```

##  Ce que fait le script
1. Crée les collections `stations` et `measurements` 
2. Crée les index :
   - `stations.id` **unique**
   - `measurements.(id_station, dh_utc)` **unique**
3. Importe les 2 fichiers JSON *format tableau* (`--jsonArray` requis si vous utilisez `mongoimport` à la main).
4. Calcule un **rapport de qualité** et l’écrit dans `data/reports/mongo_quality_report.json` :
   - **taux d’erreurs** global = nb docs non conformes / total
   - complétude des champs requis (`id_station`, `dh_utc`, `DateTime`)
   - doublons logiques (paire `id_station` + `dh_utc`)
   - valeurs hors bornes (température, humidité, pression, vent)
   - couverture référentielle (mesures qui pointent vers une station connue)

##  Logigramme (Mermaid)
Voir `Logigramme_migration.png`.

