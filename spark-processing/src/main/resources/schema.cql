CREATE KEYSPACE IF NOT EXISTS ventes_platform
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
        };

USE ventes_platform;



CREATE TABLE IF NOT EXISTS canaux_vente (
                                            canal_id        INT,
                                            code_canal      TEXT,
                                            libelle         TEXT,
                                            type            TEXT,
                                            commission_pct  DOUBLE,
                                            actif           BOOLEAN,
                                            date_creation   DATE,
                                            description     TEXT,
                                            PRIMARY KEY (canal_id)
);

CREATE TABLE IF NOT EXISTS clients (
                                       client_id        INT,
                                       prenom           TEXT,
                                       nom              TEXT,
                                       email            TEXT,
                                       telephone        TEXT,
                                       ville            TEXT,
                                       segment          TEXT,
                                       date_inscription DATE,
                                       PRIMARY KEY (client_id)
);

CREATE TABLE IF NOT EXISTS produits (
                                        produit_id   INT,
                                        code_produit TEXT,
                                        nom          TEXT,
                                        categorie    TEXT,
                                        marque       TEXT,
                                        prix_vente   DOUBLE,
                                        prix_achat   DOUBLE,
                                        poids_g      INT,
                                        sku          TEXT,
                                        statut       TEXT,
                                        PRIMARY KEY (produit_id)
);

CREATE TABLE IF NOT EXISTS stocks (
                                      produit_id          INT,
                                      depot               TEXT,
                                      stock_id            INT,
                                      code_produit        TEXT,
                                      quantite_disponible INT,
                                      quantite_reservee   INT,
                                      seuil_reappro       INT,
                                      statut              TEXT,
                                      PRIMARY KEY (produit_id, depot)
);

CREATE TABLE IF NOT EXISTS retours (
                                       client_id         INT,
                                       retour_id         INT,
                                       commande_ref      TEXT,
                                       produit_id        INT,
                                       date_retour       DATE,
                                       motif             TEXT,
                                       statut            TEXT,
                                       montant_rembourse DOUBLE,
                                       canal_achat       TEXT,
                                       PRIMARY KEY (client_id, retour_id)
);

CREATE TABLE IF NOT EXISTS ventes (
                                      canal         TEXT,
                                      vente_id      INT,
                                      reference     TEXT,
                                      date_vente    DATE,
                                      client_id     INT,
                                      produit_id    INT,
                                      quantite      INT,
                                      prix_unitaire DOUBLE,
                                      remise        DOUBLE,
                                      montant_ht    DOUBLE,
                                      tva           DOUBLE,
                                      montant_ttc   DOUBLE,
                                      region        TEXT,
                                      statut        TEXT,
                                      PRIMARY KEY (canal, vente_id)
);
