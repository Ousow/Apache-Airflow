"""
DAG : Pipeline DVF — Data Lake (HDFS) vers Data Warehouse (PostgreSQL)
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        statuts: dict = {}
        try:
            resp = requests.head(DVF_URL, timeout=10, allow_redirects=True)
            statuts["dvf_api"] = resp.status_code < 400
        except requests.RequestException as exc:
            logger.warning("API DVF inaccessible : %s", exc)
            statuts["dvf_api"] = False

        try:
            hdfs_url = f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}"
            resp_hdfs = requests.get(hdfs_url, timeout=10)
            statuts["hdfs"] = resp_hdfs.status_code == 200
        except requests.RequestException as exc:
            logger.warning("HDFS inaccessible : %s", exc)
            statuts["hdfs"] = False

        logger.info("Statut API DVF : %s", statuts["dvf_api"])
        logger.info("Statut HDFS    : %s", statuts["hdfs"])

        if not statuts["dvf_api"]:
            logger.warning("API DVF inaccessible — on continue quand même.")
            statuts["dvf_api"] = True
        if not statuts["hdfs"]:
            raise AirflowException("Cluster HDFS inaccessible — pipeline arrêté.")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        annee = datetime.now().year
        local_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.csv")
        logger.info("Début du téléchargement DVF -> %s", local_path)
        total_bytes = 0
        chunk_size = 8192

        with requests.get(DVF_URL, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with open(local_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)
                        total_bytes += len(chunk)
                        if total_bytes % (50 * 1024 * 1024) < chunk_size:
                            logger.info("Téléchargé : %.1f Mo", total_bytes / (1024 * 1024))

        taille = os.path.getsize(local_path)
        if taille < 1000:
            raise AirflowException(f"Fichier trop petit ({taille} octets).")
        logger.info("Téléchargement terminé : %.2f Mo", taille / (1024 * 1024))
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        annee = datetime.now().year
        hdfs_filename = f"dvf_{annee}.csv"
        hdfs_file_path = f"{HDFS_RAW_PATH}/{hdfs_filename}"

        mkdirs_url = f"{WEBHDFS_BASE_URL}{HDFS_RAW_PATH}/?op=MKDIRS&user.name={WEBHDFS_USER}"
        resp_mkdir = requests.put(mkdirs_url, timeout=30)
        resp_mkdir.raise_for_status()
        if not resp_mkdir.json().get("boolean"):
            raise AirflowException(f"MKDIRS échoué pour {HDFS_RAW_PATH}")
        logger.info("Répertoire HDFS prêt : %s", HDFS_RAW_PATH)

        create_url = f"{WEBHDFS_BASE_URL}{hdfs_file_path}?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
        resp1 = requests.put(create_url, allow_redirects=False, timeout=30)
        if resp1.status_code != 307:
            raise AirflowException(f"WebHDFS CREATE étape 1 : code {resp1.status_code}")
        datanode_url = resp1.headers["Location"]

        with open(local_path, "rb") as fh:
            resp2 = requests.put(
                datanode_url, data=fh,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600,
            )
        if resp2.status_code != 201:
            raise AirflowException(f"WebHDFS CREATE étape 2 : code {resp2.status_code}")

        os.remove(local_path)
        logger.info("Fichier stocké dans HDFS : %s", hdfs_file_path)
        return hdfs_file_path

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        import zipfile
        import pandas as pd

        open_url = f"{WEBHDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={WEBHDFS_USER}"
        resp = requests.get(open_url, allow_redirects=True, timeout=300)
        resp.raise_for_status()
        logger.info("Fichier lu depuis HDFS (%d octets)", len(resp.content))

        chunks = []
        with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
            noms = zf.namelist()
            logger.info("Fichiers dans le ZIP : %s", noms)
            nom_fichier = next(n for n in noms if n.endswith((".txt", ".csv")))
            logger.info("Lecture de : %s", nom_fichier)
            with zf.open(nom_fichier) as f:
                reader = pd.read_csv(
                    f, sep="|", encoding="latin-1", dtype=str,
                    engine="python", on_bad_lines="skip", chunksize=50000,
                )
                for chunk in reader:
                    chunk.columns = (
                        chunk.columns.str.strip().str.lower()
                             .str.replace(" ", "_", regex=False)
                             .str.replace(r"[éèê]", "e", regex=True)
                             .str.replace(r"[â]", "a", regex=True)
                    )
                    if "code_postal" in chunk.columns:
                        chunk = chunk[chunk["code_postal"].astype(str).str.match(r"^750\d{2}$")]
                    if "type_local" in chunk.columns:
                        chunk = chunk[chunk["type_local"] == "Appartement"]
                    if "nature_mutation" in chunk.columns:
                        chunk = chunk[chunk["nature_mutation"] == "Vente"]
                    if len(chunk) > 0:
                        chunks.append(chunk)

        if not chunks:
            logger.warning("Aucune transaction Paris/Appartement/Vente trouvée !")
            return {"agregats": [], "stats_globales": {}}

        df = pd.concat(chunks, ignore_index=True)
        nb_avant = len(df)
        logger.info("Lignes Paris/Appart/Vente : %d", nb_avant)

        for col in ["valeur_fonciere", "surface_reelle_bati"]:
            if col in df.columns:
                df[col] = df[col].str.replace(",", ".", regex=False).pipe(pd.to_numeric, errors="coerce")

        if "surface_reelle_bati" in df.columns:
            df = df[df["surface_reelle_bati"].between(9, 500)]
        if "valeur_fonciere" in df.columns:
            df = df[df["valeur_fonciere"] > 10_000]

        nb_apres = len(df)
        logger.info("Lignes après filtrage : %d (supprimées : %d)", nb_apres, nb_avant - nb_apres)

        if nb_apres == 0:
            logger.warning("Aucune transaction après filtrage numérique !")
            return {"agregats": [], "stats_globales": {}}

        df = df[df["surface_reelle_bati"] > 0].copy()
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        def code_postal_vers_arrdt(cp: str) -> int:
            n = int(str(cp).strip()[3:])
            return 16 if n > 20 else (n if n > 0 else 1)

        df["arrondissement"] = df["code_postal"].apply(code_postal_vers_arrdt)

        if "date_mutation" in df.columns:
            df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
            df["annee"] = df["date_mutation"].dt.year
            df["mois"]  = df["date_mutation"].dt.month
        else:
            df["annee"] = datetime.now().year
            df["mois"]  = datetime.now().month

        agregats_df = (
            df.groupby(["code_postal", "arrondissement", "annee", "mois"])
            .agg(
                prix_m2_moyen   = ("prix_m2", "mean"),
                prix_m2_median  = ("prix_m2", "median"),
                prix_m2_min     = ("prix_m2", "min"),
                prix_m2_max     = ("prix_m2", "max"),
                nb_transactions = ("prix_m2", "count"),
                surface_moyenne = ("surface_reelle_bati", "mean"),
            )
            .reset_index()
        )

        agregats = agregats_df.to_dict(orient="records")
        stats_globales = {
            "annee": int(df["annee"].mode()[0]) if not df["annee"].isna().all() else datetime.now().year,
            "mois":  int(df["mois"].mode()[0])  if not df["mois"].isna().all()  else datetime.now().month,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris":  float(df["prix_m2"].median()),
            "prix_m2_moyen_paris":   float(df["prix_m2"].mean()),
            "arrdt_plus_cher":  int(agregats_df.sort_values("prix_m2_median", ascending=False).iloc[0]["arrondissement"]),
            "arrdt_moins_cher": int(agregats_df.sort_values("prix_m2_median").iloc[0]["arrondissement"]),
            "surface_mediane":  float(df["surface_reelle_bati"].median()),
        }
        logger.info("Agrégation : %d arrondissements | Médiane Paris : %.0f €/m²",
                    len(agregats), stats_globales["prix_m2_median_paris"])
        return {"agregats": agregats, "stats_globales": stats_globales}

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})
        nb_total = 0

        upsert_prix = """
            INSERT INTO prix_m2_arrondissement
                (code_postal, arrondissement, annee, mois,
                 prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
                 nb_transactions, surface_moyenne, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
                prix_m2_moyen   = EXCLUDED.prix_m2_moyen,
                prix_m2_median  = EXCLUDED.prix_m2_median,
                prix_m2_min     = EXCLUDED.prix_m2_min,
                prix_m2_max     = EXCLUDED.prix_m2_max,
                nb_transactions = EXCLUDED.nb_transactions,
                surface_moyenne = EXCLUDED.surface_moyenne,
                updated_at      = NOW();
        """

        conn = hook.get_conn()
        cur = conn.cursor()

        for row in agregats:
            cur.execute(upsert_prix, (
                str(row["code_postal"]), int(row["arrondissement"]),
                int(row["annee"]), int(row["mois"]),
                float(row["prix_m2_moyen"]), float(row["prix_m2_median"]),
                float(row["prix_m2_min"]), float(row["prix_m2_max"]),
                int(row["nb_transactions"]), float(row["surface_moyenne"]),
            ))
            nb_total += 1

        if stats_globales:
            cur.execute("""
                INSERT INTO stats_marche
                    (annee, mois, nb_transactions_total,
                     prix_m2_median_paris, prix_m2_moyen_paris,
                     arrdt_plus_cher, arrdt_moins_cher, surface_mediane)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (annee, mois) DO UPDATE SET
                    nb_transactions_total = EXCLUDED.nb_transactions_total,
                    prix_m2_median_paris  = EXCLUDED.prix_m2_median_paris,
                    prix_m2_moyen_paris   = EXCLUDED.prix_m2_moyen_paris,
                    arrdt_plus_cher       = EXCLUDED.arrdt_plus_cher,
                    arrdt_moins_cher      = EXCLUDED.arrdt_moins_cher,
                    surface_mediane       = EXCLUDED.surface_mediane,
                    date_calcul           = NOW();
            """, (
                int(stats_globales.get("annee", datetime.now().year)),
                int(stats_globales.get("mois", datetime.now().month)),
                int(stats_globales.get("nb_transactions_total", 0)),
                float(stats_globales.get("prix_m2_median_paris", 0)),
                float(stats_globales.get("prix_m2_moyen_paris", 0)),
                int(stats_globales.get("arrdt_plus_cher", 0)),
                int(stats_globales.get("arrdt_moins_cher", 0)),
                float(stats_globales.get("surface_mediane", 0)),
            ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info("PostgreSQL : %d arrondissements insérés/mis à jour", nb_total)
        return nb_total

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        annee = datetime.now().year
        mois  = datetime.now().month

        rows = hook.get_records("""
            SELECT arrondissement, prix_m2_median, prix_m2_moyen,
                   nb_transactions, surface_moyenne
            FROM prix_m2_arrondissement
            WHERE annee = %s AND mois = %s
            ORDER BY prix_m2_median DESC LIMIT 20;
        """, parameters=(annee, mois))

        ordinal = {i: f"{i}e" for i in range(2, 21)}
        ordinal[1] = "1er"
        sep = "-" * 70
        lines = [
            f"\n{'='*70}",
            f"  CLASSEMENT ARRONDISSEMENTS PARISIENS — {mois:02d}/{annee}",
            f"  ({nb_inseres} arrondissements insérés/mis à jour)",
            "=" * 70, sep,
        ]
        for row in rows:
            arrdt, median, moyen, nb, surf = row
            lines.append(
                f"  {ordinal.get(arrdt, str(arrdt)):^6} | "
                f"{median:>10,.0f} €/m² | {moyen:>10,.0f} €/m² | "
                f"{nb:>6} tx | {surf:>6.1f} m²"
            )
        lines.append(sep)
        rapport = "\n".join(lines)
        logger.info(rapport)
        return rapport

    @task(task_id="analyser_tendances")
    def analyser_tendances(rapport: str) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records("""
            SELECT a.arrondissement, a.prix_m2_median, b.prix_m2_median,
                   ROUND(((a.prix_m2_median - b.prix_m2_median)
                          / NULLIF(b.prix_m2_median, 0)) * 100, 2)
            FROM prix_m2_arrondissement a
            JOIN prix_m2_arrondissement b
                ON a.arrondissement = b.arrondissement
               AND a.annee = b.annee AND a.mois = b.mois + 1
            WHERE a.annee = EXTRACT(YEAR FROM NOW())
            ORDER BY 4 DESC;
        """)
        if not rows:
            logger.info("Pas assez de données pour les tendances.")
            return "Aucune tendance calculable"
        lines = ["\n  TENDANCES MENSUELLES", "-" * 50]
        for arrdt, prix_c, prix_p, var in rows:
            t = "↑" if var and var > 0 else ("↓" if var and var < 0 else "→")
            lines.append(f"  {arrdt:>2}e : {prix_c:>8,.0f} €/m² ({t} {var:+.1f}%)")
        result = "\n".join(lines)
        logger.info(result)
        return result

    @task(task_id="rafraichir_vue_materialisee")
    def rafraichir_vue_materialisee(nb_inseres: int) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dvf_evolution_mensuelle;")
            conn.commit()
        count = hook.get_first("SELECT COUNT(*) FROM dvf_evolution_mensuelle")[0]
        logger.info("Vue matérialisée rafraîchie : %d lignes", count)
        return count

    # ── Enchaînement ─────────────────────────────────────────────────────────
    t_verif     = verifier_sources()
    t_dl        = telecharger_dvf(t_verif)
    t_hdfs      = stocker_hdfs_raw(t_dl)
    t_traiter   = traiter_donnees(t_hdfs)
    t_pg        = inserer_postgresql(t_traiter)
    t_vue       = rafraichir_vue_materialisee(t_pg)
    t_rapport   = generer_rapport(t_pg)
    t_tendances = analyser_tendances(t_rapport)

    chain(t_verif, t_dl, t_hdfs, t_traiter, t_pg, t_vue, t_rapport, t_tendances)


pipeline_dvf()