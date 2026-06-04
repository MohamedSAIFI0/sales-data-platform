package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.example.batch.BatchPipelineApp;
import org.example.batch.SparkSessionFactory;
import org.example.streaming.KafkaConsumerApp;
import org.example.utils.SchemaInitializer;

import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SchemaInitializer.initialize();

        SparkSession spark = SparkSessionFactory.create("sales-platform");

        new BatchPipelineApp().run(spark);

        new KafkaConsumerApp().run(spark);
    }
}