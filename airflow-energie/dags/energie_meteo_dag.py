from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
import logging
import json
from datetime import date
import time

# Timezone Paris pour RTE
local_tz = pendulum.timezone("Europe/Paris")
# Coordonnées des 5 régions cibles
REGIONS = {
        "Île-de-France":        {"lat": 48.8566, "lon": 2.3522},
        "Occitanie":            {"lat": 43.6047, "lon": 1.4442},
        "Nouvelle-Aquitaine":   {"lat": 44.8378, "lon": -0.5792},
        "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
        "Hauts-de-France":      {"lat": 50.6292, "lon": 3.0573},
    }
default_args = {
        "owner": "rte-data-team",
        "depends_on_past": False,
        "email_on_failure": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "sla": timedelta(minutes=90),
    }

# implémenter verifier_apis()
def verifier_apis(**context):
    """
    Vérifie la disponibilité des APIs Open-Meteo et éCO2mix.
    Lève une exception si une API est indisponible pour bloquer le pipeline.
    """
    apis = {
        "Open-Meteo": (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522"
            "&daily=sunshine_duration&timezone=Europe/Paris&forecast_days=1"
        ),
        "éCO2mix": (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
            "/eco2mix-regional-cons-def/records?limit=1&timezone=Europe%2FParis"
        ),
    }

    for nom, url in apis.items():
        try:
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                logging.info(f"{nom} est disponible (status 200).")
            else:
                raise ValueError(f"{nom} indisponible (status code {response.status_code})")

        except Exception as e:
            raise ValueError(f"Erreur lors de l'appel à l'API {nom} : {str(e)}")

    logging.info("Toutes les APIs sont disponibles. Pipeline autorisé à continuer.")

# Implémenter collecter_meteo_regions()
import requests
import logging

def collecter_meteo_regions(**context):
    """
    Collecte pour chaque région : durée d'ensoleillement (h) et vitesse max du vent (km/h)
    Retourne un dictionnaire {region: {ensoleillement_h: float, vent_kmh: float}}.
    Ce dictionnaire sera automatiquement stocké dans XCom via le return.
    """
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    resultats = {}

    for region, coords in REGIONS.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }

        try:
            # 1. Appel API
            response = requests.get(BASE_URL, params=params, timeout=15)

            # 2. Vérification HTTP
            response.raise_for_status()

            # 3. Parsing JSON
            data = response.json()

            daily = data.get("daily", {})

            # 4. Extraction ensoleillement (en secondes → heures)
            sunshine_seconds = daily["sunshine_duration"][0]
            ensoleillement_h = sunshine_seconds / 3600

            # 5. Extraction vent
            vent_kmh = daily["wind_speed_10m_max"][0]

            # 6. Stockage
            resultats[region] = {
                "ensoleillement_h": ensoleillement_h,
                "vent_kmh": vent_kmh
            }

            # 7. Logging
            logging.info(
                f"{region} -> ensoleillement: {ensoleillement_h:.2f} h, vent: {vent_kmh} km/h"
            )

        except Exception as e:
            logging.error(f"Erreur pour la région {region} : {str(e)}")
            raise

    return resultats

# Implémenter collecter_production_electrique()
def collecter_production_electrique(**context):
    """
    Collecte depuis éCO2mix la production solaire et éolienne par région.
    Agrège les valeurs pour obtenir une moyenne par région.
    Retourne un dict {region: {solaire_mw: float, eolien_mw: float}}.
    """

    BASE_URL = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
        "/eco2mix-regional-cons-def/records"
    )

    params = {
        "limit": 100,
        "timezone": "Europe/Paris",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        records = data.get("results", [])

        # Initialisation accumulation
        accumulation = {
            region: {"solaire": [], "eolien": []}
            for region in REGIONS
        }

        # Parcours des enregistrements
        for rec in records:
            region_name = rec.get("libelle_region")

            if region_name in REGIONS:
                solaire = float(rec.get("solaire") or 0.0)
                eolien = float(rec.get("eolien") or 0.0)

                accumulation[region_name]["solaire"].append(solaire)
                accumulation[region_name]["eolien"].append(eolien)

        def safe_float(value):
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0
            
        solaire = safe_float(rec.get("solaire"))
        eolien = safe_float(rec.get("eolien"))

        # Calcul des moyennes
        production = {}

        for region, values in accumulation.items():
            solaire_list = values["solaire"]
            eolien_list = values["eolien"]

            if solaire_list:
                solaire_moy = sum(solaire_list) / len(solaire_list)
            else:
                solaire_moy = 0.0

            if eolien_list:
                eolien_moy = sum(eolien_list) / len(eolien_list)
            else:
                eolien_moy = 0.0

            production[region] = {
                "solaire_mw": solaire_moy,
                "eolien_mw": eolien_moy
            }

            logging.info(
                f"{region} -> solaire: {solaire_moy:.2f} MW, eolien: {eolien_moy:.2f} MW"
            )

        return production

    except Exception as e:
        logging.error(f"Erreur collecte production électrique: {str(e)}")
        raise

#  Implémenter analyser_correlation()
def analyser_correlation(**context):
    """
    Corrèle les données météo et les données de production.
    Règles métier RTE :
      - Si ensoleillement > 6h  ET production solaire <= 1000 MW  → ALERTE solaire
      - Si vent > 30 km/h       ET production éolienne <= 2000 MW → ALERTE éolien
    Retourne un dict d'alertes par région, stocké dans XCom.
    """

    ti = context["ti"]

    # Récupération des XCom
    donnees_meteo = ti.xcom_pull(task_ids="collecter_meteo_regions")
    donnees_production = ti.xcom_pull(task_ids="collecter_production_electrique")

    alertes = {}

    for region in REGIONS:

        meteo = donnees_meteo.get(region, {})
        production = donnees_production.get(region, {})

        alertes_region = []

        ensoleillement = meteo.get("ensoleillement_h", 0)
        vent = meteo.get("vent_kmh", 0)
        solaire = production.get("solaire_mw", 0)
        eolien = production.get("eolien_mw", 0)

        # Règle 1 : solaire
        if ensoleillement > 6 and solaire <= 1000:
            alertes_region.append(
                f"ALERTE SOLAIRE : {ensoleillement:.1f}h de soleil "
                f"mais seulement {solaire:.0f} MW produits"
            )

        # Règle 2 : éolien
        if vent > 30 and eolien <= 2000:
            alertes_region.append(
                f"ALERTE ÉOLIEN : vent à {vent:.1f} km/h "
                f"mais seulement {eolien:.0f} MW produits"
            )

        # Bonus : anomalie de données
        if solaire > 0 and ensoleillement == 0:
            alertes_region.append(
                "ANOMALIE : production solaire détectée sans ensoleillement mesuré"
            )

        alertes[region] = {
            "alertes": alertes_region,
            "ensoleillement_h": ensoleillement,
            "vent_kmh": vent,
            "solaire_mw": solaire,
            "eolien_mw": eolien,
            "statut": "ALERTE" if alertes_region else "OK",
        }

    nb_alertes = sum(1 for r in alertes.values() if r["statut"] == "ALERTE")

    logging.warning(f"{nb_alertes} région(s) en alerte sur {len(REGIONS)} analysées.")

    return alertes


# Implémenter generer_rapport_energie()
def generer_rapport_energie(**context):
    """
    Génère un rapport JSON et affiche un tableau comparatif dans les logs Airflow.
    Sauvegarde le rapport dans /tmp/rapport_energie_<YYYY-MM-DD>.json.
    Retourne le chemin du fichier généré (stocké dans XCom).
    """

    time.sleep(60) 
    
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation")

    today = date.today().isoformat()

    # --- Affichage tableau dans les logs Airflow ---
    print("\n" + "=" * 80)
    print(f"  RAPPORT ENERGIE & METEO — RTE — {today}")
    print("=" * 80)

    print(
        f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} "
        f"{'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>8}"
    )

    print("-" * 80)

    for region, data in analyse.items():
        print(
            f"{region:<25} "
            f"{data['ensoleillement_h']:>10.1f} "
            f"{data['vent_kmh']:>12.1f} "
            f"{data['solaire_mw']:>13.0f} "
            f"{data['eolien_mw']:>12.0f} "
            f"{data['statut']:>8}"
        )

    print("=" * 80 + "\n")

    # --- Construction du rapport JSON ---
    rapport = {
        "date": today,
        "source": "RTE eCO2mix + Open-Meteo",
        "pipeline": "energie_meteo_dag",
        "regions": analyse,
        "resume": {
            "nb_regions_analysees": len(analyse),
            "nb_alertes": sum(1 for r in analyse.values() if r["statut"] == "ALERTE"),
            "regions_en_alerte": [
                r for r, d in analyse.items() if d["statut"] == "ALERTE"
            ],
        },
    }

    # --- Sauvegarde fichier ---
    chemin = f"/opt/airflow/dags/rapport_energie_{today}.json"

    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    logging.info(f"Rapport généré : {chemin}")

    return chemin

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback déclenché en cas de dépassement de SLA.
    """

    import logging

    logging.warning(" [SLA MISS] Détection de dépassement de SLA")

    for ti in blocking_tis:
        logging.warning(
            f"[ALERTE SLA] Tâche en retard : {ti.task_id} | "
            f"DAG : {ti.dag_id} | "
            f"Execution date : {ti.execution_date} | "
            f"Start date : {ti.start_date} | "
            f"End date : {ti.end_date}"
        )

    logging.warning(
        f"[ALERTE SLA] Nombre total de tâches en retard : {len(blocking_task_list)}"
    )

# --- Définition du DAG --
with DAG(
        dag_id="energie_meteo_dag",
        default_args=default_args,
        description="Corrélation météo / production énergétique — RTE",
          schedule="0 6 * * *",
        start_date=datetime(2024, 1, 1, tzinfo=local_tz),
        catchup=False,
        tags=["rte", "energie", "meteo", "open-data"],
        sla_miss_callback=sla_miss_callback,
    ) as dag:
        t1 = PythonOperator(
            task_id="verifier_apis",
            python_callable=verifier_apis,
        )
        t2 = PythonOperator(
            task_id="collecter_meteo_regions",
            python_callable=collecter_meteo_regions,
        )
        t3 = PythonOperator(
            task_id="collecter_production_electrique",
            python_callable=collecter_production_electrique,
        )
        t4 = PythonOperator(
            task_id="analyser_correlation",
            python_callable=analyser_correlation,
        )
        t5 = PythonOperator(
            task_id="generer_rapport_energie",
            python_callable=generer_rapport_energie,
            sla=timedelta(minutes=1),  
            
        )
        t1 >> [t2, t3] >> t4 >> t5


