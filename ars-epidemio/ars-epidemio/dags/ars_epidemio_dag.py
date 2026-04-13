from __future__ import annotations
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)


# Paramètres par défaut du DAG

default_args = {
    "owner":             "ars-occitanie",
    "depends_on_past":   False,
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# Helpers

def _semaine_from_context(context: dict) -> str:
    """Dérive le code semaine ISO (YYYY-SXX) depuis l'execution_date d'Airflow."""
    ed = context["execution_date"]
    iso_year, iso_week, _ = ed.isocalendar()
    return f"{iso_year}-S{iso_week:02d}"



# Callables des tâches


# ── Étape 2 : vérification des connexions 

def verifier_connexions(**context) -> None:
    """Vérifie que la connexion PostgreSQL ARS et les variables sont accessibles."""
    from airflow.hooks.base import BaseHook

    conn_pg = BaseHook.get_connection("postgres_ars")
    depts   = Variable.get("departements_occitanie", deserialize_json=True)
    logger.info(f"PostgreSQL : {conn_pg.host}:{conn_pg.port}/{conn_pg.schema}")
    logger.info(f"Départements configurés : {len(depts)}")


# ── Étape 4 : collecte IAS 

def collecter_donnees_ias(**context) -> str:
    semaine = _semaine_from_context(context)

    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = f"{archive_path}/raw"

    sys.path.insert(0, "/opt/airflow/scripts")
    from collecte_sursaud import (
        DATASETS_IAS,
        agreger_semaine,
        filtrer_semaine,
        telecharger_csv_ias,
    )

    resultats = {}

    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        
        logger.info(f"{syndrome} — total={len(rows_all)} | semaine={len(rows_sem)}")

        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

    output_path = f"{output_dir}/sursaud_{semaine}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "semaine": semaine,
            "syndromes": resultats
        }, f, ensure_ascii=False, indent=2)

    logger.info(f"Collecte OK → {output_path}")
    return output_path


# ── Étape 5 : archivage 

def archiver_local(**context) -> str:
    semaine = _semaine_from_context(context)
    annee, num_sem = semaine.split("-")

    chemin_source = context["task_instance"].xcom_pull(
        task_ids="collecte.collecter_donnees_sursaud"
    )

    if not chemin_source or not os.path.exists(chemin_source):
        raise FileNotFoundError(f"Fichier source introuvable : {chemin_source}")

    archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)

    chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"

    shutil.copy2(chemin_source, chemin_dest)

    logger.info(f"Archive créée : {chemin_dest}")
    return chemin_dest


def verifier_archive(**context) -> bool:
    semaine = _semaine_from_context(context)
    annee, num_sem = semaine.split("-")

    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")

    taille = os.path.getsize(chemin)

    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")

    logger.info(f"Archive valide : {chemin} ({taille} octets)")
    return True


# ── Étape 6 : calcul des indicateurs

def calculer_indicateurs_epidemiques(**context) -> str:
    semaine = _semaine_from_context(context)
    annee, num_sem = semaine.split("-")

    input_file = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    output_dir = "/data/ars/indicateurs"

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Fichier introuvable : {input_file}")

    with open(input_file, encoding="utf-8") as f:
        donnees_brutes = json.load(f)

    logger.info(f"Lecture OK : {input_file}")

    sys.path.insert(0, "/opt/airflow/scripts")
    from calcul_indicateurs import calculer_indicateurs, sauvegarder_indicateurs

    seuil_alerte_z  = float(Variable.get("seuil_alerte_zscore",  default_var="1.5"))
    seuil_urgence_z = float(Variable.get("seuil_urgence_zscore", default_var="3.0"))

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    resultats = []

    for syndrome, donnees in donnees_brutes["syndromes"].items():

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT valeur_ias
                    FROM donnees_hebdomadaires
                    WHERE syndrome = %s
                    ORDER BY semaine DESC
                    LIMIT 4
                """, (syndrome,))
                series_db = [row[0] for row in cur.fetchall() if row[0] is not None]

        indicateur = calculer_indicateurs(
            donnees, series_db, seuil_alerte_z, seuil_urgence_z
        )

        resultats.append(indicateur)

    chemin = sauvegarder_indicateurs(resultats, semaine, output_dir)

    logger.info(f"Indicateurs OK → {chemin}")
    return chemin


# ── Étape 7 : insertion PostgreSQL 

def inserer_donnees_postgres(**context) -> None:
    semaine = _semaine_from_context(context)
    annee, num_sem = semaine.split("-")

    raw_path = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    indicateurs_path = f"/data/ars/indicateurs/indicateurs_{semaine}.json"

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"RAW introuvable : {raw_path}")

    with open(raw_path, encoding="utf-8") as f:
        donnees_brutes = json.load(f)

    with open(indicateurs_path, encoding="utf-8") as f:
        indicateurs = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            for syndrome, d in donnees_brutes["syndromes"].items():

                logger.info(f"{syndrome} → IAS={d.get('valeur_ias')} | jours={d.get('nb_jours')}")

                cur.execute("""
                    INSERT INTO donnees_hebdomadaires
                    (semaine, syndrome, valeur_ias, nb_jours_donnees)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (semaine, syndrome) DO UPDATE SET
                        valeur_ias = EXCLUDED.valeur_ias,
                        nb_jours_donnees = EXCLUDED.nb_jours_donnees;
                """, (
                    semaine,
                    syndrome,
                    d.get("valeur_ias"),
                    d.get("nb_jours", 0),
                ))

        conn.commit()

    logger.info("Insertion PostgreSQL OK")

# ── Étape 8 : branchement épidémique 

def evaluer_situation_epidemique(**context) -> str:
    """
    Lit les indicateurs de la semaine en base et sélectionne la branche
    d'exécution selon la situation la plus sévère observée.

    Returns:
        task_id de la branche à exécuter.
    """
    semaine = _semaine_from_context(context)
    hook    = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT statut, COUNT(*) AS nb
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
                """,
                (semaine,),
            )
            resultats = {row[0]: row[1] for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", 0)
    nb_alerte  = resultats.get("ALERTE",  0)

    context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
    context["task_instance"].xcom_push(key="nb_alerte",  value=nb_alerte)

    logger.info(f"Semaine {semaine} → {nb_urgence} syndromes URGENCE, {nb_alerte} ALERTE")

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    if nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    return "confirmer_situation_normale"


def declencher_alerte_ars(**context) -> None:
    """Déclenche l'alerte ARS en cas d'URGENCE (log + hook notification futur)."""
    semaine    = _semaine_from_context(context)
    nb_urgence = context["task_instance"].xcom_pull(
        task_ids="evaluer_situation_epidemique", key="nb_urgence"
    )
    logger.critical(
        f" ALERTE ARS DÉCLENCHÉE — Semaine {semaine} — "
        f"{nb_urgence} syndrome(s) en URGENCE épidémique"
    )
    print(f"ALERTE_ARS:{semaine}:URGENCE:{nb_urgence}")


def envoyer_bulletin_surveillance(**context) -> None:
    """Envoie le bulletin de surveillance hebdomadaire en cas d'ALERTE."""
    semaine   = _semaine_from_context(context)
    nb_alerte = context["task_instance"].xcom_pull(
        task_ids="evaluer_situation_epidemique", key="nb_alerte"
    )
    logger.warning(
        f"  Bulletin de surveillance — Semaine {semaine} — "
        f"{nb_alerte} syndrome(s) en ALERTE"
    )
    print(f"BULLETIN:{semaine}:ALERTE:{nb_alerte}")


def confirmer_situation_normale(**context) -> None:
    """Confirme la situation normale, aucune action requise."""
    semaine = _semaine_from_context(context)
    logger.info(f" Situation épidémiologique normale — Semaine {semaine}")
    print(f"NORMAL:{semaine}")


# ── Étape 9 : génération du rapport 

def generer_rapport_hebdomadaire(**context) -> None:
    """
    Génère le rapport JSON structuré, le sauvegarde dans le volume Docker
    et l'insère dans la table rapports_ars de PostgreSQL.

    TriggerRule : NONE_FAILED_MIN_ONE_SUCCESS — s'exécute quelle que soit la branche.
    """
    semaine        = _semaine_from_context(context)
    annee, num_sem = semaine.split("-")
    hook           = PostgresHook(postgres_conn_id="postgres_ars")

    # Récupération des indicateurs + noms des départements
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ie.syndrome,
                       ie.valeur_ias,
                       ie.z_score,
                       ie.r0_estime,
                       ie.statut,
                       ie.nb_saisons_reference
                FROM indicateurs_epidemiques ie
                WHERE ie.semaine = %s
                ORDER BY
                    CASE ie.statut
                        WHEN 'URGENCE' THEN 1
                        WHEN 'ALERTE'  THEN 2
                        ELSE 3
                    END,
                    ie.valeur_ias DESC NULLS LAST
                """,
                (semaine,),
            )
            indicateurs = cur.fetchall()

    statuts           = [row[4] for row in indicateurs]
    situation_globale = (
        "URGENCE" if "URGENCE" in statuts
        else "ALERTE" if "ALERTE" in statuts
        else "NORMAL"
    )

    syndromes_urgence = [row[0] for row in indicateurs if row[4] == "URGENCE"]
    syndromes_alerte  = [row[0] for row in indicateurs if row[4] == "ALERTE"]

    recommandations_map: Dict[str, List[str]] = {
        "URGENCE": [
            "Activation du plan de réponse épidémique régional",
            "Renforcement des équipes de surveillance dans les services d'urgences",
            "Communication renforcée auprès des professionnels de santé libéraux",
            "Notification immédiate à Santé Publique France et au Ministère de la Santé",
        ],
        "ALERTE": [
            "Surveillance renforcée des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance aux partenaires de santé",
            "Vérification des capacités d'accueil des services d'urgences",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point épidémiologique dans 7 jours",
        ],
    }

    rapport: dict = {
        "semaine":                     semaine,
        "region":                      "Occitanie",
        "code_region":                 "76",
        "date_generation":             datetime.utcnow().isoformat(),
        "situation_globale":           situation_globale,
        "nb_syndromes_surveilles":     len(indicateurs),
        "syndromes_en_urgence":        syndromes_urgence,
        "syndromes_en_alerte":         syndromes_alerte,
        "indicateurs": [
            {
                "syndrome":            row[0],
                "valeur_ias":          row[1],
                "z_score":             row[2],
                "r0_estime":           row[3],
                "statut":              row[4],
                "nb_saisons_reference": row[5],
            }
            for row in indicateurs
        ],
        "recommandations":             recommandations_map[situation_globale],
        "genere_par":                  "ars_epidemio_dag v1.0",
        "pipeline_version":            "2.8",
    }

    # Sauvegarde locale dans le volume Docker
    local_dir  = f"/data/ars/rapports/{annee}/{num_sem}"
    local_path = f"{local_dir}/rapport_{semaine}.json"
    os.makedirs(local_dir, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    # Insertion PostgreSQL
    sql_rapport = """
        INSERT INTO rapports_ars
            (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence,
             rapport_json, chemin_local)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (semaine) DO UPDATE SET
            situation_globale = EXCLUDED.situation_globale,
            nb_depts_alerte   = EXCLUDED.nb_depts_alerte,
            nb_depts_urgence  = EXCLUDED.nb_depts_urgence,
            rapport_json      = EXCLUDED.rapport_json,
            chemin_local      = EXCLUDED.chemin_local,
            updated_at        = CURRENT_TIMESTAMP;
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_rapport, (
                semaine,
                situation_globale,
                len(syndromes_alerte),
                len(syndromes_urgence),
                json.dumps(rapport, ensure_ascii=False),
                local_path,
            ))
        conn.commit()

    logger.info(f"Rapport {semaine} généré → {local_path} — Statut : {situation_globale}")
    print(f"RAPPORT_OK:{local_path}:{situation_globale}")



# Définition du DAG


with DAG(
    dag_id          = "ars_epidemio_dag",
    default_args    = default_args,
    description     = "Pipeline surveillance épidémiologique ARS Occitanie — IAS® Grippe & GEA",
    schedule_interval = "0 6 * * 1",   # Tous les lundis à 6h UTC
    start_date      = datetime(2024, 1, 1),
    catchup         = True,
    max_active_runs = 1,
    tags            = ["sante-publique", "epidemio", "ias", "occitanie"],
) as dag:

    # ── Étape 3 : initialisation de la base 
    init_base_donnees = PostgresOperator(
        task_id        = "init_base_donnees",
        postgres_conn_id = "postgres_ars",
        sql            = "sql/init_ars_epidemio.sql",
        autocommit     = True,
    )

    # ── Étape 4 : collecte IAS 
    with TaskGroup(group_id="collecte") as tg_collecte:
        collecter_sursaud = PythonOperator(
            task_id          = "collecter_donnees_sursaud",
            python_callable  = collecter_donnees_ias,
            provide_context  = True,
        )

    # ── Étape 5 : archivage local
    with TaskGroup(group_id="persistance_brute") as tg_persistance_brute:
        archiver = PythonOperator(
            task_id         = "archiver_local",
            python_callable = archiver_local,
            provide_context = True,
        )
        verifier = PythonOperator(
            task_id         = "verifier_archive",
            python_callable = verifier_archive,
            provide_context = True,
        )
        archiver >> verifier

    # ── Étape 6 : calcul des indicateurs 
    with TaskGroup(group_id="traitement") as tg_traitement:
        calculer_indicateurs_task = PythonOperator(
            task_id         = "calculer_indicateurs_epidemiques",
            python_callable = calculer_indicateurs_epidemiques,
            provide_context = True,
        )

    # ── Étape 7 : persistance PostgreSQL
    with TaskGroup(group_id="persistance_operationnelle") as tg_persistance_op:
        inserer_postgres = PythonOperator(
            task_id         = "inserer_donnees_postgres",
            python_callable = inserer_donnees_postgres,
            provide_context = True,
        )

    # ── Étape 8 : branchement épidémique 
    evaluer = BranchPythonOperator(
        task_id         = "evaluer_situation_epidemique",
        python_callable = evaluer_situation_epidemique,
        provide_context = True,
    )

    alerte_ars = PythonOperator(
        task_id         = "declencher_alerte_ars",
        python_callable = declencher_alerte_ars,
        provide_context = True,
    )
    bulletin = PythonOperator(
        task_id         = "envoyer_bulletin_surveillance",
        python_callable = envoyer_bulletin_surveillance,
        provide_context = True,
    )
    normale = PythonOperator(
        task_id         = "confirmer_situation_normale",
        python_callable = confirmer_situation_normale,
        provide_context = True,
    )

    # ── Étape 9 : rapport hebdomadaire 
    generer_rapport = PythonOperator(
        task_id         = "generer_rapport_hebdomadaire",
        python_callable = generer_rapport_hebdomadaire,
        provide_context = True,
        trigger_rule    = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Dépendances 
    (
        init_base_donnees
        >> tg_collecte
        >> tg_persistance_brute
        >> tg_traitement
        >> tg_persistance_op
        >> evaluer
        >> [alerte_ars, bulletin, normale]
        >> generer_rapport
    )
