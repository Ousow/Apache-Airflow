from __future__ import annotations
#!/usr/bin/env python3
"""
Calcul des indicateurs épidémiques IAS® — ARS Occitanie

Calcule pour chaque syndrome :
  - Z-score par rapport aux N-1..N-5 saisons historiques
  - R0 simplifié (modèle SIR) sur les 4 dernières semaines IAS disponibles
  - Classification : NORMAL / ALERTE / URGENCE

Usage autonome :
    SEMAINE_CIBLE=2024-S04 \
    INPUT_FILE=/data/ars/raw/ias_2024-S04.json \
    DB_HOST=localhost DB_PORT=5433 DB_NAME=ars_epidemio \
    DB_USER=postgres DB_PASSWORD=postgres \
    python calcul_indicateurs.py
"""

import json
import logging
import os
from typing import Dict, List, Optional

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# Durée infectieuse (jours) par syndrome — utilisée pour le R0 SIR simplifié
DUREE_INFECTIEUSE: Dict[str, int] = {
    "GRIPPE":   5,
    "GEA":      3,
    "SG":       5,
    "BRONCHIO": 7,
    "COVID19":  7,
}

# Noms des colonnes historiques (ordre croissant de l'ancienneté)
SAISONS_COLS = [
    "Sais_2019_2020",
    "Sais_2020_2021",
    "Sais_2021_2022",
    "Sais_2022_2023",
    "Sais_2023_2024",
]


# Fonctions de calcul

def calculer_zscore(
    valeur_actuelle: float,
    historique: List[Optional[float]],
) -> Optional[float]:
    """
    Calcule le z-score de la valeur IAS actuelle par rapport aux saisons historiques.
    Requiert au minimum 3 valeurs non-None.

    Returns:
        z-score arrondi à 3 décimales, ou None si historique insuffisant.
    """
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        logger.warning(f"Historique insuffisant ({len(valeurs_valides)} saisons) — z-score non calculé")
        return None

    moyenne    = float(np.mean(valeurs_valides))
    ecart_type = float(np.std(valeurs_valides, ddof=1))

    if ecart_type == 0.0:
        return 0.0

    z = (valeur_actuelle - moyenne) / ecart_type
    return round(z, 4)


def classifier_statut_ias(
    valeur_ias: float,
    seuil_min: Optional[float],
    seuil_max: Optional[float],
) -> str:
    """
    Classifie selon les seuils MIN/MAX du dataset IAS.

    Returns:
        'URGENCE' | 'ALERTE' | 'NORMAL'
    """
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(
    z_score: Optional[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> str:
    """
    Classifie selon le z-score par rapport à l'historique des saisons.

    Returns:
        'URGENCE' | 'ALERTE' | 'NORMAL'
    """
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    """
    Retient le niveau le plus sévère entre les deux critères (IAS et z-score).

    Returns:
        'URGENCE' | 'ALERTE' | 'NORMAL'
    """
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(
    series_hebdomadaire: List[Optional[float]],
    duree_infectieuse: int = 5,
) -> Optional[float]:
    """
    Estimation du R0 par calcul du taux de croissance moyen sur les séries IAS.

    Formule SIR simplifiée :
        R0 = 1 + (taux_croissance_moyen * duree_infectieuse / 7)

    Args:
        series_hebdomadaire: valeurs IAS des dernières semaines (ordre chronologique)
        duree_infectieuse:   durée infectieuse en jours selon le syndrome

    Returns:
        R0 estimé (≥ 0), ou None si données insuffisantes.
    """
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]
    if len(series_valides) < 2:
        logger.warning("Série trop courte pour estimer le R0")
        return None

    croissances = [
        (series_valides[i] - series_valides[i - 1]) / series_valides[i - 1]
        for i in range(1, len(series_valides))
    ]
    taux_moyen = float(np.mean(croissances))
    r0 = max(0.0, 1 + taux_moyen * (duree_infectieuse / 7))
    return round(r0, 4)


# Calcul principal

def calculer_indicateurs(
    donnees_syndrome: dict,
    series_historique_db: List[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> dict:
    """
    Calcule l'ensemble des indicateurs pour un syndrome/semaine donné.

    Args:
        donnees_syndrome:    données agrégées du syndrome (sorties de collecte_sursaud)
        series_historique_db: dernières valeurs IAS hebdomadaires en base (pour R0)
        seuil_alerte_z:      seuil z-score pour ALERTE
        seuil_urgence_z:     seuil z-score pour URGENCE

    Returns:
        dict avec tous les indicateurs calculés.
    """
    syndrome   = donnees_syndrome["syndrome"]
    semaine    = donnees_syndrome["semaine"]
    valeur_ias = donnees_syndrome.get("valeur_ias")
    seuil_min  = donnees_syndrome.get("seuil_min")
    seuil_max  = donnees_syndrome.get("seuil_max")
    historique = donnees_syndrome.get("historique", {})
    nb_jours   = donnees_syndrome.get("nb_jours", 0)

    if valeur_ias is None:
        logger.warning(f"{syndrome} — semaine {semaine} : valeur IAS manquante, indicateurs non calculés")
        return {
            "semaine":             semaine,
            "syndrome":            syndrome,
            "valeur_ias":          None,
            "z_score":             None,
            "r0_estime":           None,
            "nb_saisons_reference": 0,
            "statut":              "NORMAL",
            "statut_ias":          "NORMAL",
            "statut_zscore":       "NORMAL",
            "commentaire":         "Données IAS manquantes pour cette semaine",
        }

    # Valeurs historiques dans l'ordre chronologique
    hist_values: List[Optional[float]] = [historique.get(col) for col in SAISONS_COLS]
    nb_saisons_valides = sum(1 for v in hist_values if v is not None)

    # Z-score
    z_score = calculer_zscore(valeur_ias, hist_values)

    # R0 (sur la série courante + historique N-1)
    duree_inf = DUREE_INFECTIEUSE.get(syndrome, 5)
    r0 = calculer_r0_simplifie(series_historique_db, duree_inf)

    # Classification
    statut_ias    = classifier_statut_ias(valeur_ias, seuil_min, seuil_max)
    statut_zscore = classifier_statut_zscore(z_score, seuil_alerte_z, seuil_urgence_z)
    statut_final  = classifier_statut_final(statut_ias, statut_zscore)

    commentaire_parts = []
    if nb_jours < 7:
        commentaire_parts.append(f"Agrégation partielle ({nb_jours}/7 jours)")
    if nb_saisons_valides < 3:
        commentaire_parts.append("Z-score non calculé (historique insuffisant)")
    if statut_final == "URGENCE":
        commentaire_parts.append("⚠️ SEUIL D'URGENCE DÉPASSÉ — Mobilisation immédiate requise")
    elif statut_final == "ALERTE":
        commentaire_parts.append("⚠️ Seuil d'alerte dépassé — Surveillance renforcée")

    return {
        "semaine":              semaine,
        "syndrome":             syndrome,
        "valeur_ias":           valeur_ias,
        "z_score":              z_score,
        "r0_estime":            r0,
        "nb_saisons_reference": nb_saisons_valides,
        "statut":               statut_final,
        "statut_ias":           statut_ias,
        "statut_zscore":        statut_zscore,
        "commentaire":          " | ".join(commentaire_parts) if commentaire_parts else None,
    }


def sauvegarder_indicateurs(
    indicateurs: List[dict],
    semaine: str,
    output_dir: str,
) -> str:
    """Sauvegarde les indicateurs calculés en JSON."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"indicateurs_{semaine}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(indicateurs, f, ensure_ascii=False, indent=2)

    logger.info(f"Indicateurs sauvegardés : {output_path}")
    return output_path


# Point d'entrée autonome

if __name__ == "__main__":
    import psycopg2

    semaine_cible = os.environ.get("SEMAINE_CIBLE", "2024-S04")
    input_file    = os.environ.get("INPUT_FILE", f"/data/ars/raw/ias_{semaine_cible}.json")
    output_dir    = os.environ.get("OUTPUT_DIR", "/data/ars/indicateurs")

    # Connexion PostgreSQL
    conn = psycopg2.connect(
        host     = os.environ.get("DB_HOST",     "localhost"),
        port     = int(os.environ.get("DB_PORT", "5433")),
        dbname   = os.environ.get("DB_NAME",     "ars_epidemio"),
        user     = os.environ.get("DB_USER",     "postgres"),
        password = os.environ.get("DB_PASSWORD", "postgres"),
    )

    with open(input_file, encoding="utf-8") as f:
        donnees_brutes = json.load(f)

    resultats = []
    for syndrome, donnees in donnees_brutes["syndromes"].items():
        # Récupérer les 4 dernières valeurs IAS en base pour calculer le R0
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT valeur_ias FROM donnees_hebdomadaires
                WHERE syndrome = %s
                ORDER BY annee DESC, numero_semaine DESC
                LIMIT 4
                """,
                (syndrome,),
            )
            series_db = [row[0] for row in cur.fetchall() if row[0] is not None]
            series_db.reverse()  # ordre chronologique

        indicateur = calculer_indicateurs(donnees, series_db)
        resultats.append(indicateur)
        logger.info(
            f"{syndrome} — IAS={donnees.get('valeur_ias')} "
            f"z={indicateur['z_score']} R0={indicateur['r0_estime']} "
            f"→ {indicateur['statut']}"
        )

    conn.close()
    chemin = sauvegarder_indicateurs(resultats, semaine_cible, output_dir)
    print(f"CALCUL_OK:{chemin}")
