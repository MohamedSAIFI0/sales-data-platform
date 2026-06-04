package org.example.batch.bronze;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.batch.SparkSessionFactory;

import java.util.List;

/**
 * BRONZE — Lecture des fichiers JSON bruts déposés par Kafka Connect dans MinIO
 *          et écriture en Delta Lake (overwrite).
 *
 *  Source  : s3a://sales-data/<topic>/year=.../month=.../day=.../hour=.../*.json
 *  Dest    : s3a://lakehouse/bronze/<topic>/
 */
public class BronzeIngestion {

    private static final String SRC  = "s3a://sales-data/topics";
    private static final String DEST = "s3a://lakehouse/bronze";

    private static final List<String> TOPICS = List.of(
            "pg_clients", "pg_produits", "canaux_vente",
            "retours", "ventes", "stocks"
    );

    public void run(SparkSession spark) {
        spark.sparkContext().setLogLevel("ERROR");

        for (String topic : TOPICS) {
            System.out.printf("[BRONZE] ingesting %s ...%n", topic);

            // Lecture récursive de tous les JSON du topic (partitionné par date)
            Dataset<Row> df = spark.read()
                    .option("recursiveFileLookup", "true")
                    .json(SRC + "/" + topic + "/");

            long count = df.count();

            df.write()
                    .format("delta")
                    .mode("overwrite")
                    .save(DEST + "/" + topic + "/");

            System.out.printf("[BRONZE] %-15s → %d rows%n", topic, count);
        }
    }
}