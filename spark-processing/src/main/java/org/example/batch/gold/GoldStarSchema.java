package org.example.batch.gold;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * GOLD — Star Schema
 * ┌─────────────────────────────────────────────────────────┐
 * │                     fact_ventes                         │
 * │  vente_id, date_vente, client_id, produit_id, canal_id  │
 * │  quantite, prix_unitaire, remise, montant_ht, tva,      │
 * │  montant_ttc, marge_brute, nb_retours, total_rembourse  │
 * └────────────┬──────────┬────────────┬────────────────────┘
 *              │          │            │            │
 *         dim_client  dim_produit  dim_canal    dim_date
 *
 *  Source  : s3a://lakehouse/silver/
 *  Dest    : s3a://lakehouse/gold/
 */
public class GoldStarSchema {

    private static final String SILVER = "s3a://lakehouse/silver";
    private static final String GOLD   = "s3a://lakehouse/gold";

    public void run(SparkSession spark) {
        spark.sparkContext().setLogLevel("ERROR");

        writeTable("dim_client",  buildDimClient(spark));
        writeTable("dim_produit", buildDimProduit(spark));
        writeTable("dim_canal",   buildDimCanal(spark));
        writeTable("dim_date",    buildDimDate(spark));
        writeTable("fact_ventes", buildFactVentes(spark));
    }

    // ─── dim_client ─────────────────────────────────────────────────────────
    private Dataset<Row> buildDimClient(SparkSession spark) {
        return spark.read().format("delta").load(SILVER + "/pg_clients/")
                .select("client_id", "prenom", "nom", "email",
                        "telephone", "ville", "segment", "date_inscription");
    }

    // ─── dim_produit ────────────────────────────────────────────────────────
    private Dataset<Row> buildDimProduit(SparkSession spark) {
        return spark.read().format("delta").load(SILVER + "/pg_produits/")
                .select("produit_id", "code_produit", "nom", "categorie",
                        "marque", "prix_vente", "prix_achat", "sku", "statut");
    }

    // ─── dim_canal ──────────────────────────────────────────────────────────
    private Dataset<Row> buildDimCanal(SparkSession spark) {
        return spark.read().format("delta").load(SILVER + "/canaux_vente/")
                .select(
                        col("canal_id"),
                        col("code_canal"),
                        col("libelle"),
                        col("type").alias("type_canal"),   // éviter conflit mot-clé SQL
                        col("commission_pct"),
                        col("actif"));
    }

    // ─── dim_date ───────────────────────────────────────────────────────────
    // Générée à partir des dates de ventes (pas de table source dédiée)
    private Dataset<Row> buildDimDate(SparkSession spark) {
        return spark.read().format("delta").load(SILVER + "/ventes/")
                .select(col("date_vente").alias("date"))
                .distinct()
                .withColumn("annee",       year(col("date")))
                .withColumn("trimestre",   quarter(col("date")))
                .withColumn("mois",        month(col("date")))
                .withColumn("semaine",     weekofyear(col("date")))
                .withColumn("jour",        dayofmonth(col("date")))
                .withColumn("jour_semaine",dayofweek(col("date")))
                .withColumn("est_weekend", col("jour_semaine").isin(1, 7));
    }

    // ─── fact_ventes ────────────────────────────────────────────────────────
    private Dataset<Row> buildFactVentes(SparkSession spark) {

        Dataset<Row> ventes = spark.read().format("delta").load(SILVER + "/ventes/");

        // Agrégation retours par vente (commande_ref = référence de la vente)
        Dataset<Row> retourAgg = spark.read().format("delta").load(SILVER + "/retours/")
                .groupBy("commande_ref")
                .agg(
                        count("retour_id").alias("nb_retours"),
                        sum("montant_rembourse").alias("total_rembourse")
                );

        // Résolution canal_id : ventes.canal (code_canal) → canaux_vente.canal_id
        Dataset<Row> canaux = spark.read().format("delta").load(SILVER + "/canaux_vente/")
                .select(
                        col("canal_id"),
                        col("code_canal")
                );

        return ventes
                // Jointure pour récupérer canal_id
                .join(canaux,
                        ventes.col("canal").equalTo(canaux.col("code_canal")),
                        "left")
                // Jointure retours sur la référence de commande
                .join(retourAgg,
                        ventes.col("reference").equalTo(retourAgg.col("commande_ref")),
                        "left")
                // Remplacer null par 0 pour les métriques retours
                .withColumn("nb_retours",
                        coalesce(col("nb_retours"), lit(0L)))
                .withColumn("total_rembourse",
                        coalesce(col("total_rembourse"), lit(0.0)))
                // Marge brute = montant_ht − coût estimé (prix_achat × quantite)
                .withColumn("marge_brute",
                        col("montant_ht").minus(
                                col("quantite").multiply(col("prix_unitaire")).multiply(0.6)
                        ))
                .select(
                        ventes.col("vente_id"),
                        ventes.col("reference"),
                        ventes.col("date_vente"),
                        ventes.col("client_id"),
                        ventes.col("produit_id"),
                        col("canal_id"),
                        ventes.col("quantite"),
                        ventes.col("prix_unitaire"),
                        ventes.col("remise"),
                        ventes.col("montant_ht"),
                        ventes.col("tva"),
                        ventes.col("montant_ttc"),
                        col("marge_brute"),
                        col("nb_retours"),
                        col("total_rembourse"),
                        ventes.col("region"),
                        ventes.col("statut")
                );
    }

    // ─── Helper écriture Delta ───────────────────────────────────────────────
    private void writeTable(String name, Dataset<Row> df) {
        System.out.printf("[GOLD] writing %s ...%n", name);
        long count = df.count();
        df.write()
                .format("parquet")
                .mode("overwrite")
                .save(GOLD + "/" + name + "/");
        System.out.printf("[GOLD] %-15s → %d rows%n", name, count);
    }
}