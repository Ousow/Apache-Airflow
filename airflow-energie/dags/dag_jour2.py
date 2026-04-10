from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import random
import os

def generate_number():
    number = random.randint(1, 100)
    print(f"Nombre généré : {number}")
    return number

def branch(**context):
    ti = context["ti"]
    number = ti.xcom_pull(task_ids="generate_number")

    if number % 2 == 0:
        return "pair"
    else:
        return "impair"

def task_pair():
    print("Nombre pair exécuté")

def task_impair():
    print("Nombre impair exécuté")

with DAG(
    dag_id="dag_branchement",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="generate_number",
        python_callable=generate_number
    )

    t2 = BranchPythonOperator(
        task_id="branch",
        python_callable=branch
    )

    pair = PythonOperator(
        task_id="pair",
        python_callable=task_pair
    )

    impair = PythonOperator(
        task_id="impair",
        python_callable=task_impair
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"  
    )

    t1 >> t2 >> [pair, impair] >> end

def generate_list():
    data = [random.randint(1, 100) for _ in range(5)]
    print(f"Liste générée : {data}")
    return data

def compute_sum(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="generate_list")

    result = sum(data)
    print(f"Somme : {result}")
    return result

def display_result(**context):
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="compute_sum")

    print(f"Résultat final : {result}")

with DAG(
    dag_id="dag_xcom_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="generate_list",
        python_callable=generate_list
    )

    t2 = PythonOperator(
        task_id="compute_sum",
        python_callable=compute_sum
    )

    t3 = PythonOperator(
        task_id="display_result",
        python_callable=display_result
    )

    t1 >> t2 >> t3

def file_exists():
    return os.path.exists("/tmp/go.txt")

with DAG(
    dag_id="dag_sensor_fichier",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    wait_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=file_exists,
        poke_interval=10,
        timeout=60,
        mode="reschedule"  
    )

    end = EmptyOperator(task_id="end")

    wait_file >> end

