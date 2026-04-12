-- Initialisation de la base DVF — Zone curated du pipeline

-- Création de la base de données dédiée aux données DVF
CREATE DATABASE dvf;

\c dvf;

-- Table 1 : Transactions brutes Paris (chargées depuis HDFS)
CREATE TABLE IF NOT EXISTS dvf_raw (
    id                      SERIAL PRIMARY KEY,
    date_mutation           DATE,
    nature_mutation         VARCHAR(50),
    valeur_fonciere         NUMERIC(15, 2),
    adresse_numero          VARCHAR(10),
    adresse_nom_voie        VARCHAR(200),
    code_postal             VARCHAR(10),
    nom_commune             VARCHAR(100),
    code_departement        VARCHAR(5),
    code_commune            VARCHAR(10),
    type_local              VARCHAR(50),
    surface_reelle_bati     NUMERIC(10, 2),
    nombre_pieces_principales INTEGER,
    longitude               NUMERIC(10, 6),
    latitude                NUMERIC(10, 6),
    prix_m2                 NUMERIC(10, 2),
    annee_mutation          INTEGER,
    mois_mutation           INTEGER,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour accélérer les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_dvf_raw_commune     ON dvf_raw(nom_commune);
CREATE INDEX IF NOT EXISTS idx_dvf_raw_type_local  ON dvf_raw(type_local);
CREATE INDEX IF NOT EXISTS idx_dvf_raw_date        ON dvf_raw(date_mutation);
CREATE INDEX IF NOT EXISTS idx_dvf_raw_code_postal ON dvf_raw(code_postal);


-- Table 2 : Prix agrégés par arrondissement (zone curated)
CREATE TABLE IF NOT EXISTS prix_m2_arrondissement (
    id               SERIAL PRIMARY KEY,
    code_postal      VARCHAR(10) NOT NULL,
    arrondissement   INTEGER NOT NULL,
    annee            INTEGER NOT NULL,
    mois             INTEGER NOT NULL,
    prix_m2_moyen    NUMERIC(10, 2),
    prix_m2_median   NUMERIC(10, 2),
    prix_m2_min      NUMERIC(10, 2),
    prix_m2_max      NUMERIC(10, 2),
    nb_transactions  INTEGER,
    surface_moyenne  NUMERIC(10, 2),
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (code_postal, annee, mois)
);

CREATE INDEX IF NOT EXISTS idx_prix_arrdt     ON prix_m2_arrondissement(arrondissement);
CREATE INDEX IF NOT EXISTS idx_prix_annee_mois ON prix_m2_arrondissement(annee, mois);


-- Table 3 : Statistiques globales du marché parisien
CREATE TABLE IF NOT EXISTS stats_marche (
    id                      SERIAL PRIMARY KEY,
    annee                   INTEGER NOT NULL,
    mois                    INTEGER NOT NULL,
    nb_transactions_total   INTEGER,
    prix_m2_median_paris    NUMERIC(10, 2),
    prix_m2_moyen_paris     NUMERIC(10, 2),
    arrdt_plus_cher         INTEGER,
    arrdt_moins_cher        INTEGER,
    surface_mediane         NUMERIC(10, 2),
    date_calcul             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (annee, mois)
);


-- Table 4 (Exercice 1) : Rapport qualité des runs DVF
CREATE TABLE IF NOT EXISTS dvf_qualite_runs (
    id                  SERIAL PRIMARY KEY,
    date_run            TIMESTAMP DEFAULT NOW(),
    nb_total            INTEGER,
    nb_valides          INTEGER,
    taux_validite       FLOAT,
    nb_prix_aberrant    INTEGER,
    nb_surface_aberrante INTEGER,
    nb_prix_m2_aberrant INTEGER,
    nb_doublons         INTEGER
);


-- Vue matérialisée (Exercice 3) : évolution mensuelle par commune

-- Note : CREATE MATERIALIZED VIEW IF NOT EXISTS n'existe pas en PostgreSQL.
-- On utilise un bloc DO $$ ... $$ pour contourner.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_matviews WHERE matviewname = 'dvf_evolution_mensuelle'
    ) THEN
        CREATE MATERIALIZED VIEW dvf_evolution_mensuelle AS
        SELECT
            code_postal,
            nom_commune                                       AS commune,
            EXTRACT(YEAR  FROM date_mutation)::INTEGER        AS annee,
            EXTRACT(MONTH FROM date_mutation)::INTEGER        AS mois,
            COUNT(*)                                          AS nb_transactions,
            ROUND(AVG(valeur_fonciere / NULLIF(surface_reelle_bati, 0))::NUMERIC, 0)
                                                              AS prix_moyen_m2,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY valeur_fonciere / NULLIF(surface_reelle_bati, 0)
            )                                                 AS prix_median_m2
        FROM dvf_raw
        WHERE surface_reelle_bati > 0
          AND valeur_fonciere     > 0
        GROUP BY
            code_postal,
            nom_commune,
            EXTRACT(YEAR  FROM date_mutation),
            EXTRACT(MONTH FROM date_mutation)
        WITH DATA;

        -- Index UNIQUE requis pour REFRESH CONCURRENTLY
        CREATE UNIQUE INDEX ON dvf_evolution_mensuelle (code_postal, annee, mois);
    END IF;
END $$;
