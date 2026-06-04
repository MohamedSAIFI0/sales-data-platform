--Dataset : CA par mois
SELECT
  DATE_TRUNC('month', CAST(date_vente AS DATE)) AS mois,
  SUM(montant_ttc) AS ca_total,
  COUNT(*) AS nb_ventes,
  SUM(montant_ttc - montant_ht) AS total_tva
FROM lakehouse.bronze.ventes
WHERE statut = 'completee'
GROUP BY 1
ORDER BY 1

-- Dataset : CA par canal
SELECT
  canal,
  statut,
  COUNT(*) AS nb_ventes,
  SUM(montant_ttc) AS ca_total,
  ROUND(AVG(remise) * 100, 1) AS remise_moy_pct
FROM lakehouse.bronze.ventes
GROUP BY canal, statut
ORDER BY ca_total DESC

--Dataset : Top produits
SELECT
  p.nom AS produit,
  p.categorie,
  SUM(v.quantite) AS qte_vendue,
  SUM(v.montant_ttc) AS ca,
  COUNT(*) AS nb_commandes
FROM lakehouse.bronze.ventes v
JOIN lakehouse.bronze.pg_produits p
  ON v.produit_id = p.produit_id
WHERE v.statut = 'completee'
GROUP BY p.nom, p.categorie
ORDER BY ca DESC
LIMIT 15


--Dataset : Retours par motif
SELECT
  motif,
  statut,
  COUNT(*) AS nb_retours,
  SUM(montant_rembourse) AS total_rembourse
FROM lakehouse.bronze.retours
GROUP BY motif, statut
ORDER BY nb_retours DESC

--Dataset : Analyse des stocks
SELECT
  p.nom AS produit,
  p.categorie,
  s.depot,
  s.quantite_disponible,
  s.quantite_reservee,
  s.seuil_reappro,
  s.statut,
  (s.quantite_disponible - s.seuil_reappro) AS marge_stock
FROM lakehouse.bronze.stocks s
JOIN lakehouse.bronze.pg_produits p
  ON s.produit_id = p.produit_id


--Dataset : CA par segment et ville
SELECT
  c.segment,
  c.ville,
  COUNT(DISTINCT v.client_id) AS nb_clients_actifs,
  SUM(v.montant_ttc) AS ca_segment,
  COUNT(*) AS nb_achats
FROM lakehouse.bronze.ventes v
JOIN lakehouse.bronze.pg_clients c
  ON v.client_id = c.client_id
WHERE v.statut = 'completee'
GROUP BY c.segment, c.ville
ORDER BY ca_segment DESC