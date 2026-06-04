package org.example.batch;

import org.apache.spark.sql.SparkSession;
import org.example.batch.bronze.BronzeIngestion;
import org.example.batch.silver.SilverCleaning;
import org.example.batch.gold.GoldStarSchema;

public class BatchPipelineApp {

    public void run(SparkSession spark) {
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║   SALES LAKEHOUSE — MEDALLION PIPELINE   ║");
        System.out.println("╚══════════════════════════════════════════╝");

        System.out.println("\n[1/3] BRONZE — Raw ingestion from MinIO");
        new BronzeIngestion().run(spark);

        System.out.println("\n[2/3] SILVER — Cleaning & typing");
        new SilverCleaning().run(spark);

        System.out.println("\n[3/3] GOLD — Star Schema");
        new GoldStarSchema().run(spark);

        System.out.println("\n✅ Batch pipeline terminé.");
    }
}