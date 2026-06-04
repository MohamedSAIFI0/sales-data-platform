-- =============================================================================
-- init.sql — Données référentielles Lakehouse Omnicanal
-- Tables : clients (50 lignes) + produits (30 lignes)
-- Exécution : docker exec -i postgres psql -U admin -d data_platform < init.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TABLE CLIENTS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clients (
                                       client_id        INTEGER PRIMARY KEY,
                                       prenom           VARCHAR(50)  NOT NULL,
    nom              VARCHAR(50)  NOT NULL,
    email            VARCHAR(120) NOT NULL UNIQUE,
    telephone        VARCHAR(20),
    ville            VARCHAR(50),
    segment          VARCHAR(30),
    date_inscription DATE
    );

INSERT INTO clients (client_id, prenom, nom, email, telephone, ville, segment, date_inscription) VALUES
                                                                                                     (1,  'Youssef',   'Benali',      'youssef.benali@gmail.com',      '0661-234567', 'Casablanca',  'Premium',       '2021-03-15'),
                                                                                                     (2,  'Fatima',    'El Mansouri', 'fatima.elmansouri@hotmail.com',  '0662-345678', 'Rabat',       'Standard',      '2020-07-22'),
                                                                                                     (3,  'Omar',      'Tazi',        'omar.tazi@gmail.com',           '0663-456789', 'Fès',         'Fidèle',        '2019-11-08'),
                                                                                                     (4,  'Khadija',   'Chraibi',     'khadija.chraibi@yahoo.fr',      '0664-567890', 'Marrakech',   'Occasionnel',   '2022-01-30'),
                                                                                                     (5,  'Mehdi',     'Alaoui',      'mehdi.alaoui@gmail.com',        '0665-678901', 'Tanger',      'Professionnel', '2018-06-14'),
                                                                                                     (6,  'Nadia',     'Bouhaddou',   'nadia.bouhaddou@gmail.com',     '0666-789012', 'Agadir',      'Premium',       '2021-09-03'),
                                                                                                     (7,  'Karim',     'Idrissi',     'karim.idrissi@outlook.com',     '0667-890123', 'Meknès',      'Standard',      '2020-04-18'),
                                                                                                     (8,  'Samira',    'Benkirane',   'samira.benkirane@gmail.com',    '0668-901234', 'Oujda',       'Fidèle',        '2019-08-25'),
                                                                                                     (9,  'Rachid',    'Lahlou',      'rachid.lahlou@gmail.com',       '0669-012345', 'Kénitra',     'Occasionnel',   '2022-05-11'),
                                                                                                     (10, 'Zineb',     'Squalli',     'zineb.squalli@hotmail.com',     '0661-123456', 'Tétouan',     'Premium',       '2020-12-07'),
                                                                                                     (11, 'Hamid',     'Berrada',     'hamid.berrada@gmail.com',       '0662-234567', 'Casablanca',  'Professionnel', '2018-02-19'),
                                                                                                     (12, 'Houda',     'Amrani',      'houda.amrani@gmail.com',        '0663-345678', 'Rabat',       'Standard',      '2021-07-14'),
                                                                                                     (13, 'Amine',     'Kettani',     'amine.kettani@yahoo.fr',        '0664-456789', 'Casablanca',  'Fidèle',        '2019-03-28'),
                                                                                                     (14, 'Meriem',    'Tahiri',      'meriem.tahiri@gmail.com',       '0665-567890', 'Fès',         'Premium',       '2022-08-16'),
                                                                                                     (15, 'Khalid',    'Ouazzani',    'khalid.ouazzani@outlook.com',   '0666-678901', 'Marrakech',   'Occasionnel',   '2020-10-05'),
                                                                                                     (16, 'Loubna',    'Hajji',       'loubna.hajji@gmail.com',        '0667-789012', 'Agadir',      'Standard',      '2021-04-22'),
                                                                                                     (17, 'Tariq',     'Bensouda',    'tariq.bensouda@gmail.com',      '0668-890123', 'Tanger',      'Professionnel', '2019-06-30'),
                                                                                                     (18, 'Soukaina',  'Filali',      'soukaina.filali@hotmail.com',   '0669-901234', 'Safi',        'Fidèle',        '2022-02-13'),
                                                                                                     (19, 'Adil',      'Cherkaoui',   'adil.cherkaoui@gmail.com',      '0661-012345', 'El Jadida',   'Premium',       '2020-09-07'),
                                                                                                     (20, 'Wafa',      'Bensaid',     'wafa.bensaid@gmail.com',        '0662-123456', 'Casablanca',  'Standard',      '2021-11-25'),
                                                                                                     (21, 'Hicham',    'Alami',       'hicham.alami@gmail.com',        '0663-234567', 'Rabat',       'Occasionnel',   '2019-01-17'),
                                                                                                     (22, 'Imane',     'Ziani',       'imane.ziani@yahoo.fr',          '0664-345678', 'Meknès',      'Fidèle',        '2022-06-09'),
                                                                                                     (23, 'Saad',      'Benomar',     'saad.benomar@gmail.com',        '0665-456789', 'Kénitra',     'Premium',       '2020-03-21'),
                                                                                                     (24, 'Ghita',     'Fassi',       'ghita.fassi@outlook.com',       '0666-567890', 'Casablanca',  'Professionnel', '2018-11-04'),
                                                                                                     (25, 'Driss',     'Moussaoui',   'driss.moussaoui@gmail.com',     '0667-678901', 'Oujda',       'Standard',      '2021-08-18'),
                                                                                                     (26, 'Hajar',     'Rahali',      'hajar.rahali@gmail.com',        '0668-789012', 'Tétouan',     'Occasionnel',   '2022-03-30'),
                                                                                                     (27, 'Mounir',    'Benjelloun',  'mounir.benjelloun@hotmail.com', '0669-890123', 'Casablanca',  'Fidèle',        '2019-07-12'),
                                                                                                     (28, 'Sanaa',     'Chakir',      'sanaa.chakir@gmail.com',        '0661-901234', 'Fès',         'Premium',       '2020-05-26'),
                                                                                                     (29, 'Badr',      'Lyoussi',     'badr.lyoussi@gmail.com',        '0662-012345', 'Marrakech',   'Standard',      '2021-12-03'),
                                                                                                     (30, 'Layla',     'Benchekroun', 'layla.benchekroun@yahoo.fr',    '0663-123456', 'Agadir',      'Professionnel', '2018-09-15'),
                                                                                                     (31, 'Nabil',     'El Fassi',    'nabil.elfassi@gmail.com',       '0664-234567', 'Rabat',       'Occasionnel',   '2022-07-08'),
                                                                                                     (32, 'Chaimae',   'Bakkali',     'chaimae.bakkali@gmail.com',     '0665-345678', 'Tanger',      'Fidèle',        '2020-01-19'),
                                                                                                     (33, 'Iliasse',   'Sabri',       'iliasse.sabri@outlook.com',     '0666-456789', 'Casablanca',  'Premium',       '2021-06-27'),
                                                                                                     (34, 'Rania',     'Guessous',    'rania.guessous@gmail.com',      '0667-567890', 'Meknès',      'Standard',      '2019-04-10'),
                                                                                                     (35, 'Othmane',   'Naciri',      'othmane.naciri@hotmail.com',    '0668-678901', 'Safi',        'Occasionnel',   '2022-09-22'),
                                                                                                     (36, 'Yasmine',   'Bennis',      'yasmine.bennis@gmail.com',      '0669-789012', 'El Jadida',   'Professionnel', '2018-05-07'),
                                                                                                     (37, 'Zakaria',   'Taoufiq',     'zakaria.taoufiq@gmail.com',     '0661-890123', 'Casablanca',  'Fidèle',        '2021-02-14'),
                                                                                                     (38, 'Hafsa',     'El Idrissi',  'hafsa.elidrissi@yahoo.fr',      '0662-901234', 'Rabat',       'Premium',       '2020-08-31'),
                                                                                                     (39, 'Reda',      'Amrani',      'reda.amrani@gmail.com',         '0663-012345', 'Fès',         'Standard',      '2022-04-05'),
                                                                                                     (40, 'Btissam',   'Zouiten',     'btissam.zouiten@gmail.com',     '0664-123456', 'Marrakech',   'Occasionnel',   '2019-10-23'),
                                                                                                     (41, 'Jawad',     'Senhaji',     'jawad.senhaji@outlook.com',     '0665-234567', 'Kénitra',     'Fidèle',        '2021-05-16'),
                                                                                                     (42, 'Narjiss',   'Rhoulami',    'narjiss.rhoulami@gmail.com',    '0666-345678', 'Tanger',      'Premium',       '2020-02-28'),
                                                                                                     (43, 'Younes',    'Belghiti',    'younes.belghiti@gmail.com',     '0667-456789', 'Agadir',      'Professionnel', '2018-12-11'),
                                                                                                     (44, 'Salma',     'Tahir',       'salma.tahir@hotmail.com',       '0668-567890', 'Oujda',       'Standard',      '2022-10-04'),
                                                                                                     (45, 'Ayoub',     'Benabdallah', 'ayoub.benabdallah@gmail.com',   '0669-678901', 'Casablanca',  'Occasionnel',   '2019-02-17'),
                                                                                                     (46, 'Ikram',     'Mouline',     'ikram.mouline@gmail.com',       '0661-789012', 'Rabat',       'Fidèle',        '2021-10-09'),
                                                                                                     (47, 'Khalil',    'Slaoui',      'khalil.slaoui@yahoo.fr',        '0662-890123', 'Fès',         'Premium',       '2020-06-24'),
                                                                                                     (48, 'Dounia',    'Bentouhami',  'dounia.bentouhami@gmail.com',   '0663-901234', 'Casablanca',  'Standard',      '2022-01-16'),
                                                                                                     (49, 'Ismail',    'Chaoui',      'ismail.chaoui@outlook.com',     '0664-012345', 'Marrakech',   'Professionnel', '2018-08-29'),
                                                                                                     (50, 'Soumia',    'Berroho',     'soumia.berroho@gmail.com',      '0665-123456', 'Tétouan',     'Occasionnel',   '2021-01-06');

-- -----------------------------------------------------------------------------
-- TABLE PRODUITS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS produits (
                                        produit_id   INTEGER PRIMARY KEY,
                                        code_produit VARCHAR(20)  NOT NULL UNIQUE,
    nom          VARCHAR(100) NOT NULL,
    categorie    VARCHAR(50),
    marque       VARCHAR(50),
    prix_vente   NUMERIC(10,2),
    prix_achat   NUMERIC(10,2),
    poids_g      INTEGER,
    sku          VARCHAR(20),
    statut       VARCHAR(20)
    );

INSERT INTO produits (produit_id, code_produit, nom, categorie, marque, prix_vente, prix_achat, poids_g, sku, statut) VALUES
                                                                                                                          (1,  'PROD-00001', 'Smartphone Samsung Galaxy A54',      'Électronique',    'Samsung',     2499.00, 1800.00,  195, 'SM-A54-BLK', 'Actif'),
                                                                                                                          (2,  'PROD-00002', 'Laptop HP Pavilion 15',              'Électronique',    'HP',          5999.00, 4200.00, 1800, 'HP-PAV-15',  'Actif'),
                                                                                                                          (3,  'PROD-00003', 'Écouteurs Bluetooth JBL Tune 510',   'Électronique',    'JBL',          349.00,  220.00,  162, 'JBL-T510',   'Actif'),
                                                                                                                          (4,  'PROD-00004', 'Montre connectée Xiaomi Band 8',     'Électronique',    'Xiaomi',       299.00,  180.00,   27, 'XI-BAND8',   'Actif'),
                                                                                                                          (5,  'PROD-00005', 'Tablette Lenovo Tab M10',            'Électronique',    'Lenovo',      1899.00, 1300.00,  480, 'LEN-TAB-M10','Actif'),
                                                                                                                          (6,  'PROD-00006', 'Djellaba homme brodée bleue',        'Vêtements',       'MarocStyle',   650.00,  300.00,  400, 'DJ-H-BL-M',  'Actif'),
                                                                                                                          (7,  'PROD-00007', 'Caftan femme soie rouge',            'Vêtements',       'Zouina',      1200.00,  600.00,  550, 'CF-F-RG-M',  'Actif'),
                                                                                                                          (8,  'PROD-00008', 'Sneakers Nike Air Max 90',           'Vêtements',       'Nike',         899.00,  550.00,  800, 'NK-AM90-42', 'Actif'),
                                                                                                                          (9,  'PROD-00009', 'Polo Ralph Lauren classique',        'Vêtements',       'Ralph Lauren', 750.00,  400.00,  250, 'RL-POLO-L',  'Actif'),
                                                                                                                          (10, 'PROD-00010', 'Sac à main cuir artisanal',          'Vêtements',       'ArtisanMaroc', 480.00,  200.00,  600, 'SAC-CUI-MR', 'Actif'),
                                                                                                                          (11, 'PROD-00011', 'Huile d''argan pure 100ml',           'Beauté & Santé',  'NaturaMaroc',   89.00,   35.00,  120, 'HU-ARG-100', 'Actif'),
                                                                                                                          (12, 'PROD-00012', 'Crème hydratante au lait de chamelle','Beauté & Santé', 'SaharaBeauty', 149.00,   70.00,  200, 'CR-LCH-200', 'Actif'),
                                                                                                                          (13, 'PROD-00013', 'Parfum Ambre & Oud 50ml',            'Beauté & Santé',  'OrientScent',  320.00,  150.00,  180, 'PA-OUD-50',  'Actif'),
                                                                                                                          (14, 'PROD-00014', 'Savon beldi artisanal 200g',         'Beauté & Santé',  'HammamGold',    35.00,   12.00,  200, 'SV-BLD-200', 'Actif'),
                                                                                                                          (15, 'PROD-00015', 'Complément alimentaire Vitamine D',  'Beauté & Santé',  'HealthPlus',    95.00,   45.00,   80, 'VIT-D-60',   'Actif'),
                                                                                                                          (16, 'PROD-00016', 'Couscous fin 5kg',                   'Alimentation',    'Dari',          85.00,   50.00, 5000, 'COUS-5KG',   'Actif'),
                                                                                                                          (17, 'PROD-00017', 'Huile d''olive extra vierge 1L',      'Alimentation',    'Oléa',         110.00,   65.00,  920, 'HO-EV-1L',   'Actif'),
                                                                                                                          (18, 'PROD-00018', 'Miel de thym du Moyen Atlas 500g',   'Alimentation',    'AtlasHoney',   180.00,   90.00,  600, 'MI-THY-500', 'Actif'),
                                                                                                                          (19, 'PROD-00019', 'Harissa épicée 200g',                'Alimentation',    'ChefMaroc',     25.00,   10.00,  220, 'HAR-200',    'Actif'),
                                                                                                                          (20, 'PROD-00020', 'Thé vert à la menthe 250g',          'Alimentation',    'AtlasTea',      55.00,   25.00,  260, 'THE-MEN-250','Actif'),
                                                                                                                          (21, 'PROD-00021', 'Table basse zellige ronde',          'Maison & Jardin', 'ArtisanFes',  1800.00,  900.00, 8000, 'TB-ZEL-RND', 'Actif'),
                                                                                                                          (22, 'PROD-00022', 'Lanterne marocaine cuivre',          'Maison & Jardin', 'MeknesArt',    350.00,  150.00, 1200, 'LAN-CUI-M',  'Actif'),
                                                                                                                          (23, 'PROD-00023', 'Tapis berbère laine 200x300',        'Maison & Jardin', 'BerberArt',   2500.00, 1200.00, 6000, 'TAP-BER-23', 'Actif'),
                                                                                                                          (24, 'PROD-00024', 'Coussin brodé traditionnel',         'Maison & Jardin', 'HomeDeco',      120.00,   55.00,  400, 'COU-BRO-45', 'Actif'),
                                                                                                                          (25, 'PROD-00025', 'Tajine en poterie de Salé',          'Maison & Jardin', 'PoterieSale',   180.00,   80.00, 1500, 'TAJ-POT-M',  'Actif'),
                                                                                                                          (26, 'PROD-00026', 'Vélo VTT 26 pouces Decathlon',       'Sport & Loisirs', 'BTwin',       1599.00,  950.00, 12000,'VTT-26-BL',  'Actif'),
                                                                                                                          (27, 'PROD-00027', 'Tapis de yoga antidérapant',         'Sport & Loisirs', 'SportPro',      199.00,   90.00, 1200, 'TAP-YOG-6', 'Actif'),
                                                                                                                          (28, 'PROD-00028', 'Ballon de football taille 5',        'Sport & Loisirs', 'Adidas',        149.00,   70.00,  430, 'BAL-FT-T5',  'Actif'),
                                                                                                                          (29, 'PROD-00029', 'Roman "Les Âmes perdues" (arabe)',   'Livres & Médias', 'DarNachr',       75.00,   30.00,  350, 'LIV-AR-001', 'Actif'),
                                                                                                                          (30, 'PROD-00030', 'Jeu de société Monopoly Maroc Ed.', 'Jouets',           'Hasbro',        299.00,  150.00,  900, 'JEU-MON-MA', 'Actif');

-- -----------------------------------------------------------------------------
-- Vérification
-- -----------------------------------------------------------------------------
SELECT 'clients'  AS table_name, COUNT(*) AS total FROM clients
UNION ALL
SELECT 'produits' AS table_name, COUNT(*) AS total FROM produits;