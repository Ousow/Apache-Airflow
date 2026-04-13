# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie

## Auteur
- Nom : SOW
- Prénom : Oumou
- Formation : Master 2 Data Engineering — IPSSI Montpellier
- Date : Avril 2026

---

## Présentation

Ce projet déploie une **data platform épidémiologique** pour l'ARS Occitanie.
Il collecte chaque semaine les données **IAS® (Indicateurs Avancés Sanitaires)** publiées
par OpenHealth sur data.gouv.fr, calcule des indicateurs statistiques (z-score, R0 SIR simplifié)
et génère des alertes automatisées selon la sévérité épidémique observée en région Occitanie.

---

## Prérequis

| Outil | Version minimale |
|---|---|
| Docker Desktop | 4.0 |
| Docker Compose | 2.0 |
| Python (tests locaux) | 3.11 |

---

## Instructions de déploiement

### 1. Démarrage de la stack

```bash
# Cloner 
https://github.com/Ousow/Apache-Airflow/tree/master/ars-epidemio/ars-epidemio
cd ars-epidemio/

# Créer le fichier d'environnement
echo "AIRFLOW_UID=$(id -u)" > .env

# Démarrer tous les services
docker compose up -d

# Vérifier que tous les services sont Running
docker compose ps
```

Résultat attendu :

```
NAME                               STATUS
ars-epidemio-postgres-1            running
ars-epidemio-postgres-ars-1        running
ars-epidemio-redis-1               running
ars-epidemio-airflow-webserver-1   running
ars-epidemio-airflow-scheduler-1   running
ars-epidemio-airflow-worker-1      running
ars-epidemio-flower-1              running
```

### 2. Accès aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Flower (Celery) | http://localhost:5555 | — |
| PostgreSQL ARS | localhost:5433 | postgres / postgres / ars_epidemio |

### 3. Configuration des connexions et variables Airflow

Dans l'UI Airflow : **Admin > Connections**, créer :

| Conn Id | Type | Host | Port | Schema | Login | Password |
|---|---|---|---|---|---|---|
| `postgres_ars` | Postgres | postgres-ars | 5432 | ars_epidemio | postgres | postgres |

> Note : la connexion est également injectée automatiquement via la variable d'environnement
> `AIRFLOW_CONN_POSTGRES_ARS` dans le docker-compose.yaml.

Dans **Admin > Variables**, créer :

| Clé | Valeur |
|---|---|
| `semaines_historique` | `12` |
| `seuil_alerte_incidence` | `150` |
| `seuil_urgence_incidence` | `500` |
| `seuil_alerte_zscore` | `1.5` |
| `seuil_urgence_zscore` | `3.0` |
| `departements_occitanie` | `["09","11","12","30","31","32","34","46","48","65","66","81","82"]` |
| `syndromes_surveilles` | `["GRIPPE","GEA","SG","BRONCHIO","COVID19"]` |
| `archive_base_path` | `/data/ars` |

### 4. Démarrage du pipeline

1. Dans l'UI Airflow, localiser le DAG **`ars_epidemio_dag`**
2. L'activer (toggle ON)
3. Déclencher un Run manuel via le bouton ▶ ou attendre le prochain lundi 6h UTC
4. Suivre l'avancement dans la **Graph View**

---

## Architecture des données

### Partitionnement du volume Docker

```
/data/ars/
├── raw/
│   └── <annee>/
│       └── <semaine>/          ex: 2024/S04/
│           └── sursaud_<semaine>.json
├── indicateurs/
│   └── indicateurs_<semaine>.json
└── rapports/
    └── <annee>/
        └── <semaine>/
            └── rapport_<semaine>.json
```

### Schéma PostgreSQL (base `ars_epidemio`)

| Table | Rôle |
|---|---|
| `syndromes` | Référentiel des 5 syndromes surveillés |
| `departements` | Référentiel des 13 départements d'Occitanie |
| `donnees_hebdomadaires` | Données IAS agrégées par semaine et syndrome |
| `indicateurs_epidemiques` | Z-score, R0, statut calculé |
| `rapports_ars` | Rapports JSON générés + chemin local |

Toutes les tables disposent de colonnes `created_at` / `updated_at` avec triggers automatiques
pour la traçabilité RGPD.

---

## Architecture du pipeline (DAG)

```
init_base_donnees
    └── [collecte]
            └── collecter_donnees_sursaud
                └── [persistance_brute]
                        ├── archiver_local
                        └── verifier_archive
                            └── [traitement]
                                    └── calculer_indicateurs_epidemiques
                                        └── [persistance_operationnelle]
                                                └── inserer_donnees_postgres
                                                    └── evaluer_situation_epidemique
                                                            ├── declencher_alerte_ars   (URGENCE)
                                                            ├── envoyer_bulletin_surveillance (ALERTE)
                                                            └── confirmer_situation_normale   (NORMAL)
                                                                └── generer_rapport_hebdomadaire
                                                                    (TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
```

---

## Décisions techniques

- **CeleryExecutor + Redis** : permet la distribution des tâches sur plusieurs workers et
  garantit la résilience (un worker peut tomber sans bloquer le pipeline).
- **Deux bases PostgreSQL distinctes** : isolation entre les métadonnées Airflow et les données
  métier ARS, conformément aux bonnes pratiques de sécurité.
- **Volume Docker nommé `ars-data`** : persistance garantie entre les redémarrages, partitionnement
  temporel pour faciliter les purges légales (5 ans minimum).
- **ON CONFLICT DO UPDATE** : idempotence complète — relancer un DAG Run sur la même semaine
  met à jour les données sans créer de doublons.
- **catchup=True + max_active_runs=1** : permet de rejouer les semaines passées de façon séquentielle,
  sans surcharger la stack ni créer de conditions de course.
- **Aucun credential en dur** : toutes les connexions passent par les Connections Airflow ou
  les variables d'environnement Docker, jamais dans le code DAG.

---

## Difficultés rencontrées et solutions

| Difficulté | Solution |
|---|---|
| Format CSV français (`;` et `,` décimal) | Parsing manuel avec `csv.DictReader` + remplacement `,` → `.` |
| Colonnes `annee` et `numero_semaine` générées (STORED) impossibles à insérer | Ne pas les inclure dans les `INSERT` — PostgreSQL les calcule automatiquement |
| `BranchPythonOperator` saute les tâches non sélectionnées | `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` sur `generer_rapport` pour ne pas être bloqué |
| XCom entre TaskGroups | Utiliser le `task_id` complet avec préfixe du groupe (`collecte.collecter_donnees_sursaud`) |
| Z-score non calculable si < 3 saisons | Guard clause explicite avec warning, statut `NORMAL` par défaut |

---

## Commandes utiles de debugging

```bash
# Logs en temps réel
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# Entrer dans un conteneur
docker compose exec airflow-worker bash

# Vérifier le volume de données
docker compose exec airflow-worker find /data/ars -type f | sort

# Workers Celery actifs
docker compose exec airflow-worker \
  celery --app airflow.executors.celery_executor.app inspect active

# Vérifier les tables PostgreSQL
docker compose exec postgres-ars \
  psql -U postgres -d ars_epidemio -c "\dt"

# Consommation ressources
docker stats
```

