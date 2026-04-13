#!/usr/bin/env python3
"""
Script de collecte des données IAS® (Indicateurs Avancés Sanitaires)
Grippe et Gastro-entérite — OpenHealth / data.gouv.fr

Usage autonome :
    SEMAINE_CIBLE=2024-S04 OUTPUT_DIR=/data/ars/raw python collecte_sursaud.py
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import datetime, date
from typing import Dict, List, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# URLs directes des CSV régionaux sur data.gouv.fr (licence ODbL)
DATASETS_IAS: Dict[str, str] = {
    "GRIPPE": "https://www.data.gouv.fr/api/1/datasets/r/35f46fbb-7a97-46b3-a93c-35a471033447",
    "GEA":    "https://www.data.gouv.fr/api/1/datasets/r/6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3",
}

# Occitanie = Languedoc-Roussillon (Loc_Reg91) + Midi-Pyrénées (Loc_Reg73) — codes pré-2016
COL_OCCITANIE_LR = "Loc_Reg91"  # Languedoc-Roussillon
COL_OCCITANIE_MP = "Loc_Reg73"  # Midi-Pyrénées

# Colonnes historiques des saisons précédentes
SAISONS_COLS = [
    "Sais_2023_2024",
    "Sais_2022_2023",
    "Sais_2021_2022",
    "Sais_2020_2021",
    "Sais_2019_2020",
]


def get_semaine_iso(reference_date: Optional[date] = None) -> str:
    """Retourne la semaine ISO courante au format YYYY-SXX."""
    if reference_date is None:
        reference_date = date.today()
    year, week, _ = reference_date.isocalendar()
    return f"{year}-S{week:02d}"


def telecharger_csv_ias(url: str) -> List[dict]:
    """
    Télécharge et parse un CSV IAS® depuis data.gouv.fr.
    Gère le séparateur ';' et le séparateur décimal ',' (format français).
    """
    logger.info(f"Téléchargement : {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    content = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")

    rows: List[dict] = []
    for row in reader:
        # Normalise les virgules décimales → points, et gère les valeurs manquantes
        cleaned = {
            k: v.replace(",", ".") if v not in ("NA", "", None) else None
            for k, v in row.items()
        }
        rows.append(cleaned)

    logger.info(f"{len(rows)} lignes récupérées depuis {url}")
    return rows


def filtrer_semaine(rows: List[dict], semaine: str) -> List[dict]:
    """
    Filtre les lignes correspondant à la semaine ISO demandée.
    Convertit PERIODE (DD-MM-YYYY) en numéro de semaine ISO pour comparaison.
    """
    try:
        annee_cible = int(semaine[:4])
        num_sem_cible = int(semaine[6:])  # après 'YYYY-S'
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Format de semaine invalide : '{semaine}' (attendu YYYY-SXX)") from exc

    filtered: List[dict] = []
    for row in rows:
        periode = row.get("PERIODE")
        if not periode:
            continue
        try:
            d = datetime.strptime(periode, "%d-%m-%Y").date()
            iso_year, iso_week, _ = d.isocalendar()
            if iso_year == annee_cible and iso_week == num_sem_cible:
                filtered.append(row)
        except ValueError:
            logger.debug(f"Format de date ignoré : {periode}")
            continue

    logger.info(f"{len(filtered)} jours trouvés pour la semaine {semaine}")
    return filtered


def safe_mean(values: List[float]) -> Optional[float]:
    """Calcule la moyenne d'une liste, retourne None si vide."""
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def agreger_semaine(rows: List[dict], syndrome: str, semaine: str) -> dict:
    """
    Agrège les lignes d'une semaine en une valeur IAS hebdomadaire.
    Calcule la moyenne de Loc_Reg91 + Loc_Reg73 (Occitanie pré-2016) et les moyennes des colonnes historiques.

    Returns:
        dict avec semaine, syndrome, valeur_ias, seuil_min, seuil_max,
        nb_jours, et les valeurs historiques par saison.
    """
    valeurs_ias: List[float] = []
    min_saison_vals: List[float] = []
    max_saison_vals: List[float] = []
    historique: Dict[str, List[float]] = {col: [] for col in SAISONS_COLS}

    for row in rows:
        # Valeur IAS Occitanie = moyenne(Loc_Reg91, Loc_Reg73) — codes régions pré-2016
        val_lr = row.get(COL_OCCITANIE_LR)
        val_mp = row.get(COL_OCCITANIE_MP)
        vals_occ = []
        for v in [val_lr, val_mp]:
            if v not in (None, "", "NA"):
                try:
                    try:
                        v = str(v).replace(",", ".").strip()
                        vals_occ.append(float(v))
                    except:
                        continue
                except ValueError:
                    print("DEBUG:", val_lr, val_mp)
                    pass
        
        logger.info(f"vals_occ = {vals_occ}")
        
        if vals_occ:
            valeurs_ias.append(sum(vals_occ) / len(vals_occ))
        
        logger.info(f"Colonnes disponibles: {rows[0].keys()}")

        # Seuils épidémiques du dataset
        for field, dest in [("MIN_Saison", min_saison_vals), ("MAX_Saison", max_saison_vals)]:
            v = row.get(field)
            if v is not None:
                try:
                    dest.append(float(v))
                except ValueError:
                    pass

        # Valeurs historiques (N-1 à N-5)
        for col in SAISONS_COLS:
            v = row.get(col)
            if v is not None:
                try:
                    historique[col].append(float(v))
                except ValueError:
                    pass

    return {
        "semaine":   semaine,
        "syndrome":  syndrome,
        "valeur_ias": safe_mean(valeurs_ias),
        "seuil_min": safe_mean(min_saison_vals),
        "seuil_max": safe_mean(max_saison_vals),
        "nb_jours":  len(valeurs_ias),
        "historique": {col: safe_mean(vals) for col, vals in historique.items()},
    }


def sauvegarder_donnees(donnees: Dict[str, dict], semaine: str, output_dir: str) -> str:
    """
    Sauvegarde les données IAS agrégées en JSON dans le répertoire de sortie.

    Returns:
        Chemin absolu du fichier JSON créé.
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"ias_{semaine}.json")

    payload = {
        "semaine":    semaine,
        "collecte_le": datetime.utcnow().isoformat(),
        "source":     "IAS_OpenHealth_data.gouv.fr",
        "syndromes":  donnees,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"Données sauvegardées : {output_path}")
    return output_path


if __name__ == "__main__":
    semaine_cible = os.environ.get("SEMAINE_CIBLE", get_semaine_iso())
    output_dir    = os.environ.get("OUTPUT_DIR", "/data/ars/raw")

    logger.info(f"=== Collecte IAS — semaine {semaine_cible} ===")

    resultats: Dict[str, dict] = {}
    for syndrome, url in DATASETS_IAS.items():
        try:
            rows_all = telecharger_csv_ias(url)
            rows_sem = filtrer_semaine(rows_all, semaine_cible)
            resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine_cible)
        except Exception as exc:
            logger.error(f"Échec collecte {syndrome} : {exc}")
            raise

    chemin = sauvegarder_donnees(resultats, semaine_cible, output_dir)
    print(f"COLLECTE_OK:{chemin}")
