package org.example.batch;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    private static final String MINIO_ENDPOINT   = "http://localhost:9000";
    private static final String MINIO_ACCESS_KEY = "saifi";
    private static final String MINIO_SECRET_KEY = "PSSwOrd123";

    public static SparkSession create(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                // ── Delta Lake ──────────────────────────────────────────────────────
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // ── Heartbeat / réseau (local[*]) ───────────────────────────────────
                // CORRECTION : augmente le délai de heartbeat pour que les micro-batchs
                // lents (ex. gros rattrapages Kafka) ne déclenchent plus le
                // "Exit as unable to send heartbeats to driver more than 60 times".
                .config("spark.executor.heartbeatInterval", "20s")
                .config("spark.network.timeout",            "300s")
                // ── MinIO / S3A ─────────────────────────────────────────────────────
                .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .getOrCreate();
    }
}