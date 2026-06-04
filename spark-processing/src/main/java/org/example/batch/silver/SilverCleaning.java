package org.example.batch.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

/**
 * SILVER — Nettoyage, typage et déduplication.
 * Les schémas reflètent exactement ceux définis dans KafkaConsumerApp.
 *
 *  Source  : s3a://lakehouse/bronze/<topic>/
 *  Dest    : s3a://lakehouse/silver/<topic>/
 */
public class SilverCleaning {

    private static final String BRONZE = "s3a://lakehouse/bronze";
    private static final String SILVER = "s3a://lakehouse/silver";

    public void run(SparkSession spark) {
        spark.sparkContext().setLogLevel("ERROR");

        writeTable(spark, "pg_clients",   cleanClients(spark));
        writeTable(spark, "pg_produits",  cleanProduits(spark));
        writeTable(spark, "canaux_vente", cleanCanaux(spark));
        writeTable(spark, "stocks",       cleanStocks(spark));
        writeTable(spark, "retours",      cleanRetours(spark));
        writeTable(spark, "ventes",       cleanVentes(spark));
    }

    // ─── Clients ────────────────────────────────────────────────────────────
    private Dataset<Row> cleanClients(SparkSession spark) {
        return spark.read().format("delta").load(BRONZE + "/pg_clients/")
                .dropDuplicates("client_id")
                .na().drop("any", new String[]{"client_id", "email"})
                .withColumn("client_id",        col("client_id").cast("int"))
                .withColumn("email",             lower(trim(col("email"))))
                .withColumn("date_inscription",  to_date(col("date_inscription")));
    }

    // ─── Produits ───────────────────────────────────────────────────────────
    private Dataset<Row> cleanProduits(SparkSession spark) {
        return spark.read().format("delta").load(BRONZE + "/pg_produits/")
                .dropDuplicates("produit_id")
                .na().drop("any", new String[]{"produit_id"})
                .withColumn("produit_id",  col("produit_id").cast("int"))
                .withColumn("prix_vente",  col("prix_vente").cast("double"))
                .withColumn("prix_achat",  col("prix_achat").cast("double"))
                .withColumn("poids_g",     col("poids_g").cast("int"));
    }

    // ─── Canaux de vente ────────────────────────────────────────────────────
    private Dataset<Row> cleanCanaux(SparkSession spark) {
        return spark.read().format("delta").load(BRONZE + "/canaux_vente/")
                .dropDuplicates("canal_id")
                .na().drop("any", new String[]{"canal_id"})
                .withColumn("commission_pct", col("commission_pct").cast("double"))
                .withColumn("actif",          col("actif").cast("boolean"))
                .withColumn("date_creation",  to_date(col("date_creation")));
    }

    // ─── Stocks ─────────────────────────────────────────────────────────────
    private Dataset<Row> cleanStocks(SparkSession spark) {
        return spark.read().format("delta").load(BRONZE + "/stocks/")
                .dropDuplicates("stock_id")
                .withColumn("stock_id",             col("stock_id").cast("int"))
                .withColumn("produit_id",            col("produit_id").cast("int"))
                .withColumn("quantite_disponible",   col("quantite_disponible").cast("int"))
                .withColumn("quantite_reservee",     col("quantite_reservee").cast("int"))
                .withColumn("seuil_reappro",         col("seuil_reappro").cast("int"));
    }

    // ─── Retours ────────────────────────────────────────────────────────────
    private Dataset<Row> cleanRetours(SparkSession spark) {
        return spark.read().format("delta").load(BRONZE + "/retours/")
                .dropDuplicates("retour_id")
                .na().drop("any", new String[]{"retour_id", "commande_ref"})
                .withColumn("client_id",         col("client_id").cast("int"))
                .withColumn("produit_id",         col("produit_id").cast("int"))
                .withColumn("date_retour",        to_date(col("date_retour")))
                .withColumn("montant_rembourse",  col("montant_rembourse").cast("double"));
    }

    // ─── Ventes ─────────────────────────────────────────────────────────────
    private Dataset<Row> cleanVentes(SparkSession spark) {
        return spark.read().format("delta").load(BRONZE + "/ventes/")
                .dropDuplicates("vente_id")
                .na().drop("any", new String[]{"vente_id", "client_id", "produit_id"})
                .withColumn("vente_id",    col("vente_id").cast("int"))
                .withColumn("client_id",   col("client_id").cast("int"))
                .withColumn("produit_id",  col("produit_id").cast("int"))
                .withColumn("quantite",    col("quantite").cast("int"))
                .withColumn("prix_unitaire",col("prix_unitaire").cast("double"))
                .withColumn("remise",      col("remise").cast("double"))
                .withColumn("montant_ht",  col("montant_ht").cast("double"))
                .withColumn("tva",         col("tva").cast("double"))
                .withColumn("montant_ttc", col("montant_ttc").cast("double"))
                .withColumn("date_vente",  to_date(col("date_vente")));
    }

    // ─── Helper écriture ────────────────────────────────────────────────────
    private void writeTable(SparkSession spark, String topic, Dataset<Row> df) {
        System.out.printf("[SILVER] processing %s ...%n", topic);
        long count = df.count();
        df.write()
                .format("delta")
                .mode("overwrite")
                .save(SILVER + "/" + topic + "/");
        System.out.printf("[SILVER] %-15s → %d rows%n", topic, count);
    }
}