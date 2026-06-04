package org.example.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.example.repository.CassandraWriter;
import org.example.utils.SchemaInitializer;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class KafkaConsumerApp {

    public void run(SparkSession spark) throws TimeoutException, StreamingQueryException {

        spark.sparkContext().setLogLevel("ERROR");

        SchemaInitializer.initialize();

        CassandraWriter writer = new CassandraWriter("localhost", 9042, "ventes_platform");

        Dataset<Row> kafkaDf = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "canaux_vente,pg_clients,pg_produits,retours,stocks,ventes")

                .option("startingOffsets", "earliest")

                .option("maxOffsetsPerTrigger", "5000")
                .load();

        Dataset<Row> rawDf = kafkaDf.selectExpr(
                "CAST(value AS STRING)", "topic", "timestamp");

        // ── Schémas JSON ─────────────────────────────────────────────────────────
        StructType canauxVenteSchema = new StructType()
                .add("canal_id",       DataTypes.StringType, true)
                .add("code_canal",     DataTypes.StringType, true)
                .add("libelle",        DataTypes.StringType, true)
                .add("type",           DataTypes.StringType, true)
                .add("commission_pct", DataTypes.StringType, true)
                .add("actif",          DataTypes.StringType, true)
                .add("date_creation",  DataTypes.StringType, true)
                .add("description",    DataTypes.StringType, true);

        StructType clientSchema = new StructType()
                .add("client_id",        DataTypes.IntegerType, true)
                .add("prenom",           DataTypes.StringType,  true)
                .add("nom",              DataTypes.StringType,  true)
                .add("email",            DataTypes.StringType,  true)
                .add("telephone",        DataTypes.StringType,  true)
                .add("ville",            DataTypes.StringType,  true)
                .add("segment",          DataTypes.StringType,  true)
                .add("date_inscription", DataTypes.DateType,    true);

        StructType produitSchema = new StructType()
                .add("produit_id",   DataTypes.IntegerType, true)
                .add("code_produit", DataTypes.StringType,  true)
                .add("nom",          DataTypes.StringType,  true)
                .add("categorie",    DataTypes.StringType,  true)
                .add("marque",       DataTypes.StringType,  true)
                .add("prix_vente",   DataTypes.DoubleType,  true)
                .add("prix_achat",   DataTypes.DoubleType,  true)
                .add("poids_g",      DataTypes.IntegerType, true)
                .add("sku",          DataTypes.StringType,  true)
                .add("statut",       DataTypes.StringType,  true);

        StructType stockSchema = new StructType()
                .add("stock_id",             DataTypes.IntegerType, true)
                .add("produit_id",           DataTypes.IntegerType, true)
                .add("code_produit",         DataTypes.StringType,  true)
                .add("depot",                DataTypes.StringType,  true)
                .add("quantite_disponible",  DataTypes.IntegerType, true)
                .add("quantite_reservee",    DataTypes.IntegerType, true)
                .add("seuil_reappro",        DataTypes.IntegerType, true)
                .add("statut",               DataTypes.StringType,  true);

        StructType retourSchema = new StructType()
                .add("retour_id",        DataTypes.StringType, true)
                .add("commande_ref",     DataTypes.StringType, true)
                .add("produit_id",       DataTypes.StringType, true)
                .add("client_id",        DataTypes.StringType, true)
                .add("date_retour",      DataTypes.StringType, true)
                .add("motif",            DataTypes.StringType, true)
                .add("statut",           DataTypes.StringType, true)
                .add("montant_rembourse",DataTypes.StringType, true)
                .add("canal_achat",      DataTypes.StringType, true);

        StructType venteSchema = new StructType()
                .add("vente_id",     DataTypes.IntegerType, true)
                .add("reference",    DataTypes.StringType,  true)
                .add("date_vente",   DataTypes.DateType,    true)
                .add("client_id",    DataTypes.IntegerType, true)
                .add("produit_id",   DataTypes.IntegerType, true)
                .add("quantite",     DataTypes.IntegerType, true)
                .add("prix_unitaire",DataTypes.DoubleType,  true)
                .add("remise",       DataTypes.DoubleType,  true)
                .add("montant_ht",   DataTypes.DoubleType,  true)
                .add("tva",          DataTypes.DoubleType,  true)
                .add("montant_ttc",  DataTypes.DoubleType,  true)
                .add("canal",        DataTypes.StringType,  true)
                .add("region",       DataTypes.StringType,  true)
                .add("statut",       DataTypes.StringType,  true);

        Dataset<Row> canauxFinal  = parse(rawDf, "canaux_vente", canauxVenteSchema);
        Dataset<Row> clientsFinal = parse(rawDf, "pg_clients",   clientSchema);
        Dataset<Row> produitsFinal= parse(rawDf, "pg_produits",  produitSchema);
        Dataset<Row> stocksFinal  = parse(rawDf, "stocks",       stockSchema);
        Dataset<Row> retoursFinal = parse(rawDf, "retours",      retourSchema);
        Dataset<Row> ventesFinal  = parse(rawDf, "ventes",       venteSchema);


        writer.writeCanauxVente(canauxFinal,  "/tmp/checkpoints/canaux_vente");
        writer.writeClients    (clientsFinal, "/tmp/checkpoints/clients");
        writer.writeProduits   (produitsFinal,"/tmp/checkpoints/produits");
        writer.writeStocks     (stocksFinal,  "/tmp/checkpoints/stocks");
        writer.writeRetours    (retoursFinal, "/tmp/checkpoints/retours");

        StreamingQuery lastQuery = writer.writeVentes(ventesFinal, "/tmp/checkpoints/ventes");
        lastQuery.awaitTermination();
    }

    private static Dataset<Row> parse(Dataset<Row> rawDf,
                                      String topic,
                                      StructType schema) {
        return rawDf
                .filter(col("topic").equalTo(topic))
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*");
    }
}