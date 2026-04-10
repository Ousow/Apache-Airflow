from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import subprocess
import logging
import os
import requests
from airflow.sensors.base import BaseSensorOperator
from datetime import timedelta


# CONFIG
SEUIL_ERREUR_PCT = 5.0

# --- TASK 1 
def generer_logs_journaliers(**context):
    execution_date = context["ds"]

    fichier_sortie = f"/shared/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    os.makedirs("/shared", exist_ok=True)

    logging.info(f"Génération logs pour {execution_date}")

    result = subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        capture_output=True,
        text=True
    )

    logging.info(result.stdout)
    logging.error(result.stderr)

    if result.returncode != 0:
        raise Exception("Erreur dans generer_logs.py")

    return fichier_sortie


# --- ALERT / ARCHIVE 
def alerter_equipe_ops(**context):
    execution_date = context["ds"]
    logging.warning(f"[ALERTE] Taux d'erreur élevé pour {execution_date}")


def archiver_rapport_ok(**context):
    execution_date = context["ds"]
    logging.info(f"[OK] Taux d'erreur normal pour {execution_date}")


# --- BRANCHING 
def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

    logging.info(f"Lecture du fichier: {fichier_taux}")

    with open(fichier_taux, "r") as f:
        contenu = f.read()

    parts = contenu.split()

    erreurs = 0
    total = 0

    for p in parts:
        if "ERREURS=" in p:
            erreurs = int(p.split("=")[1])
        if "TOTAL=" in p:
            total = int(p.split("=")[1])

    taux_pct = (erreurs / total) * 100 if total > 0 else 0

    logging.info(f"Taux d'erreur = {taux_pct:.2f}%")

    if taux_pct > SEUIL_ERREUR_PCT:
        return "alerter_equipe_ops"
    else:
        return "archiver_rapport_ok"

# --- HdfsFileSensor
class HdfsFileSensor(BaseSensorOperator):
    def __init__(self, host, path, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.path = path

    def poke(self, context):
        url = f"{self.host}/webhdfs/v1{self.path}?op=GETFILESTATUS"

        try:
            response = requests.get(url)

            if response.status_code == 200:
                self.log.info(f"Fichier trouvé : {self.path}")
                return True
            else:
                self.log.info("Fichier non trouvé, retry...")
                return False

        except Exception as e:
            self.log.warning(f"Erreur connexion HDFS : {e}")
            return False

# --- tâche instable
def tache_instable(**context):
    tentative = context["ti"].try_number

    logging.info(f"Tentative n°{tentative}")

    if tentative < 3:
        raise Exception(f"[SIMULÉ] HDFS indisponible — tentative {tentative}")

    logging.info(f"✓ Connexion HDFS rétablie à la tentative {tentative}")

    return "OK"

# --- Callback retry
def log_retry(context):
    ti = context["task_instance"]
    logging.warning(
        f"Retry n°{ti.try_number} pour la task {ti.task_id}"
    )

# --- DAG 
default_args = {
    "retries": 4,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="log_ecommerce_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # --- GENERATION 
    t_generate = PythonOperator(
        task_id="generer_logs",
        python_callable=generer_logs_journaliers,
        provide_context=True
    )

    # --- UPLOAD HDFS 
    t_upload = BashOperator(
        task_id="uploader_vers_hdfs",
        bash_command="""
        set -e

        EXECUTION_DATE="{{ ds }}"
        FICHIER_LOCAL="/shared/access_${EXECUTION_DATE}.log"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        CONTENEUR_NAMENODE="namenode"

        echo "[INFO] Vérification fichier"
        if [ ! -f "${FICHIER_LOCAL}" ]; then
            echo "[ERROR] Fichier introuvable"
            exit 1
        fi

        echo "[INFO] Upload vers HDFS"
        docker exec ${CONTENEUR_NAMENODE} hdfs dfs -mkdir -p /data/ecommerce/logs/raw
        docker exec ${CONTENEUR_NAMENODE} hdfs dfs -put -f ${FICHIER_LOCAL} ${CHEMIN_HDFS}

        echo "[INFO] Vérification"
        docker exec ${CONTENEUR_NAMENODE} hdfs dfs -ls /data/ecommerce/logs/raw
        """
    )

    ## On ne peut pas faire directement docker exec namenode hdfs dfs -put /tmp/access_*.log ... parce que les 
    # containers ont des filesystem isolés ## /tmp/access_*.log existe dans le container Airflow (scheduler) mais 
    # le container namenode a son propre /tmp ## 
    # Donc pour namenode /tmp/access_*.log n'existe pas 
    
    # Le problème avec le montage de volumes entre les conteneurs airflow-scheduler et namenode dans ce 
    # docker-compose est qu'il n’y a aucun point de partage entre les deux containers. 
    # Airflow écrit le fichier dans /tmp (container interne) alors que Namenode ne peut pas y accéder 
    
    ## La solution est d'ajouter un volume commun entre Airflow et namenode.

    # --- instabilité
    t_instable = PythonOperator(
    task_id="tache_instable",
    python_callable=tache_instable,
    retries=2,
    retry_delay=timedelta(seconds=10),
    on_retry_callback=log_retry,
    )
    
    # --- SENSOR HDFS 
    t_sensor = HdfsFileSensor(
        task_id="hdfs_file_sensor",
        host="http://namenode:9870",
        #path="/data/ecommerce/logs/raw/access_{{ ds }}.log",
        #path="/data/ecommerce/logs/raw/access_2026-04-09.log"
        path="/data/ecommerce/logs/raw/fichier_inexistant.log",
        poke_interval=30,
        timeout=600,
        mode="reschedule"
    )

    # --- ANALYSE 
    t_analyser = BashOperator(
    task_id="analyser_logs_hdfs",
    bash_command="""
    set -e

    EXECUTION_DATE="{{ ds }}"
    CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
    FICHIER_LOCAL="/tmp/logs_analyse_${EXECUTION_DATE}.log"

    echo "[INFO] Lecture du fichier HDFS"

    docker exec namenode hdfs dfs -cat "${CHEMIN_HDFS}" > ${FICHIER_LOCAL}

    echo "[INFO] Calcul du taux d'erreur"

    TOTAL=$(wc -l < ${FICHIER_LOCAL})
    ERREURS=$(grep -cP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" (4|5)[0-9]{2}' ${FICHIER_LOCAL} || true)

    echo "Total: ${TOTAL}, Erreurs: ${ERREURS}"

    echo "ERREURS=${ERREURS} TOTAL=${TOTAL}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
    """
    )
    
    t_archiver = BashOperator(
    task_id="archiver_logs_hdfs",
    bash_command="""
    set -e

    EXECUTION_DATE="{{ ds }}"
    SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
    DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
    CONTENEUR_NAMENODE="namenode"

    echo "[INFO] Déplacement HDFS : ${SOURCE} → ${DESTINATION}"

    docker exec ${CONTENEUR_NAMENODE} hdfs dfs -mkdir -p /data/ecommerce/logs/processed
    docker exec ${CONTENEUR_NAMENODE} hdfs dfs -mv ${SOURCE} ${DESTINATION}

    echo "[OK] Fichier archivé dans la zone processed"
    """,
    trigger_rule="none_failed_min_one_success",
    
    )

    # --- BRANCH 
    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur
    )

    # --- BRANCH TASKS 
    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok
    )

    # --- DEPENDENCIES 
    t_generate >> t_upload >> t_sensor >> t_analyser >> t_branch

    t_branch >> t_alerte >> t_archiver
    t_branch >> t_archive_ok >> t_archiver