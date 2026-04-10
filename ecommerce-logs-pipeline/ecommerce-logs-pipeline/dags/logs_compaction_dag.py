from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
import subprocess
import logging
import os

t_wait_ingestion = ExternalTaskSensor(
    task_id="attendre_fin_ingestion",
    external_dag_id="log_ecommerce_dag",
    external_task_id="archiver_logs_hdfs",  
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    mode="reschedule",
    poke_interval=30,
    timeout=600
)

with DAG(
    dag_id="logs_compaction_dag",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["compaction"],
) as dag:

    def lister_fichiers_semaine(**context):
        cmd = "docker exec namenode hdfs dfs -ls /data/ecommerce/logs/processed/"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        fichiers = []
        for line in result.stdout.split("\n"):
            if ".log" in line:
                fichiers.append(line.split()[-1])

        logging.info(f"Fichiers trouvés: {fichiers}")
        return fichiers


    def fusionner_fichiers(**context):
        fichiers = context["ti"].xcom_pull(task_ids="lister_fichiers")

        output = "/tmp/weekly.log"

        cmd = f"docker exec namenode hdfs dfs -cat {' '.join(fichiers)} > {output}"
        subprocess.run(cmd, shell=True)

        return output


    def verifier_compaction(**context):
        fichier = context["ti"].xcom_pull(task_ids="fusionner_fichiers")

        if not os.path.exists(fichier):
            raise Exception("Fichier non créé")

        taille = os.path.getsize(fichier)

        if taille == 0:
            raise Exception("Fichier vide")

        logging.info("Compaction OK")


    def supprimer_fichiers_journaliers(**context):
        cmd = "docker exec namenode hdfs dfs -rm /data/ecommerce/logs/processed/access_*.log"
        subprocess.run(cmd, shell=True)

        logging.info("Fichiers journaliers supprimés")


    t1 = PythonOperator(
        task_id="lister_fichiers",
        python_callable=lister_fichiers_semaine
    )

    t2 = PythonOperator(
        task_id="fusionner_fichiers",
        python_callable=fusionner_fichiers
    )

    t3 = PythonOperator(
        task_id="verifier_compaction",
        python_callable=verifier_compaction
    )

    t4 = PythonOperator(
        task_id="supprimer_fichiers",
        python_callable=supprimer_fichiers_journaliers
    )

    t_wait_ingestion >> t1 >> t2 >> t3 >> t4