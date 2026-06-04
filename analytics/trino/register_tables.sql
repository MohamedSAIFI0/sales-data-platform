-- Exécuter dans Trino CLI :
-- docker exec -it trino trino

-- 1. Créer le schéma
CREATE SCHEMA IF NOT EXISTS lakehouse.gold
WITH (location = 's3a://lakehouse/gold/');

-- 2. Enregistrer les tables (EXTERNAL = Trino lit les fichiers Parquet existants)

CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_ventes (
    vente_id        INTEGER,
    reference       VARCHAR,
    date_vente      DATE,
    client_id       INTEGER,
    produit_id      INTEGER,
    canal_id        VARCHAR,
    quantite        INTEGER,
    prix_unitaire   DOUBLE,
    remise          DOUBLE,
    montant_ht      DOUBLE,
    tva             DOUBLE,
    montant_ttc     DOUBLE,
    marge_brute     DOUBLE,
    nb_retours      BIGINT,
    total_rembourse DOUBLE,
    region          VARCHAR,
    statut          VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/gold/fact_ventes/'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_client (
    client_id        INTEGER,
    prenom           VARCHAR,
    nom              VARCHAR,
    email            VARCHAR,
    telephone        VARCHAR,
    ville            VARCHAR,
    segment          VARCHAR,
    date_inscription DATE
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/gold/dim_client/'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_produit (
    produit_id   INTEGER,
    code_produit VARCHAR,
    nom          VARCHAR,
    categorie    VARCHAR,
    marque       VARCHAR,
    prix_vente   DOUBLE,
    prix_achat   DOUBLE,
    sku          VARCHAR,
    statut       VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/gold/dim_produit/'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_canal (
    canal_id       VARCHAR,
    code_canal     VARCHAR,
    libelle        VARCHAR,
    type_canal     VARCHAR,
    commission_pct DOUBLE,
    actif          BOOLEAN
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/gold/dim_canal/'
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
    date         DATE,
    annee        INTEGER,
    trimestre    INTEGER,
    mois         INTEGER,
    semaine      INTEGER,
    jour         INTEGER,
    jour_semaine INTEGER,
    est_weekend  BOOLEAN
) WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/gold/dim_date/'
);

-- 3. Vérification rapide
SELECT COUNT(*) FROM lakehouse.gold.fact_ventes;