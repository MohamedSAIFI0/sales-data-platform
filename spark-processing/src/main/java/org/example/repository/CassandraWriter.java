package org.example.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.concurrent.TimeoutException;

public class CassandraWriter {

    private final String cassandraHost;
    private final int    cassandraPort;
    private final String keyspace;

    public CassandraWriter(String cassandraHost, int cassandraPort, String keyspace) {
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        this.keyspace      = keyspace;
    }

    // ── Helpers pour données numériques natives (ventes, stocks, clients, produits) ──

    private static void setInt(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name); else b.setInt(name, row.getInt(idx));
    }

    private static void setDouble(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name); else b.setDouble(name, row.getDouble(idx));
    }

    private static void setString(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name); else b.setString(name, row.getString(idx));
    }

    private static void setDate(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name);
        else {
            LocalDate ld = java.sql.Date.valueOf(row.getDate(idx).toString()).toLocalDate();
            b.setLocalDate(name, ld);
        }
    }

    // ── Helpers pour données CSV (valeurs stockées en String dans le JSON) ──────────

    private static void setIntS(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name);
        else b.setInt(name, Integer.parseInt(row.getString(idx).trim()));
    }

    private static void setDoubleS(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name);
        else b.setDouble(name, Double.parseDouble(row.getString(idx).trim()));
    }

    private static void setBooleanS(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name);
        else b.setBoolean(name, Boolean.parseBoolean(row.getString(idx).trim()));
    }

    private static void setDateS(BoundStatementBuilder b, String name, Row row) {
        int idx = row.fieldIndex(name);
        if (row.isNullAt(idx)) b.setToNull(name);
        else b.setLocalDate(name, LocalDate.parse(row.getString(idx).trim()));
    }

    // ── Helper commun : ouvre une CqlSession par partition ───────────────────────────
    // CORRECTION : la session est ouverte une fois par partition (pas une fois par row),
    // ce qui était déjà le cas — conservé tel quel, c'est la bonne pratique.

    private CqlSession openSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter("datacenter1")
                .withKeyspace(keyspace)
                .build();
    }

    // ── Writes ───────────────────────────────────────────────────────────────────────
    // CORRECTION : chaque méthode accepte un paramètre checkpointLocation.
    // Sans checkpoint Spark relit tous les offsets Kafka depuis le début après un crash.

    public StreamingQuery writeCanauxVente(Dataset<Row> df,
                                           String checkpointLocation) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        batch.filter(batch.col("canal_id").isNotNull())
                                .foreachPartition(rows -> {
                                    try (CqlSession s = openSession()) {
                                        PreparedStatement ps = s.prepare(
                                                "INSERT INTO canaux_vente " +
                                                        "(canal_id,code_canal,libelle,type,commission_pct,actif,date_creation,description) " +
                                                        "VALUES (:canal_id,:code_canal,:libelle,:type,:commission_pct,:actif,:date_creation,:description)");
                                        rows.forEachRemaining(row -> {
                                            BoundStatementBuilder b = ps.boundStatementBuilder();
                                            setIntS   (b, "canal_id",       row);
                                            setString (b, "code_canal",     row);
                                            setString (b, "libelle",        row);
                                            setString (b, "type",           row);
                                            setDoubleS(b, "commission_pct", row);
                                            setBooleanS(b,"actif",          row);
                                            setDateS  (b, "date_creation",  row);
                                            setString (b, "description",    row);
                                            s.execute(b.build());
                                        });
                                    }
                                })
                ).start();
    }

    public StreamingQuery writeClients(Dataset<Row> df,
                                       String checkpointLocation) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        batch.filter(batch.col("client_id").isNotNull())
                                .foreachPartition(rows -> {
                                    try (CqlSession s = openSession()) {
                                        PreparedStatement ps = s.prepare(
                                                "INSERT INTO clients " +
                                                        "(client_id,prenom,nom,email,telephone,ville,segment,date_inscription) " +
                                                        "VALUES (:client_id,:prenom,:nom,:email,:telephone,:ville,:segment,:date_inscription)");
                                        rows.forEachRemaining(row -> {
                                            BoundStatementBuilder b = ps.boundStatementBuilder();
                                            setInt   (b, "client_id",        row);
                                            setString(b, "prenom",           row);
                                            setString(b, "nom",              row);
                                            setString(b, "email",            row);
                                            setString(b, "telephone",        row);
                                            setString(b, "ville",            row);
                                            setString(b, "segment",          row);
                                            setDate  (b, "date_inscription", row);
                                            s.execute(b.build());
                                        });
                                    }
                                })
                ).start();
    }

    public StreamingQuery writeProduits(Dataset<Row> df,
                                        String checkpointLocation) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        batch.filter(batch.col("produit_id").isNotNull())
                                .foreachPartition(rows -> {
                                    try (CqlSession s = openSession()) {
                                        PreparedStatement ps = s.prepare(
                                                "INSERT INTO produits " +
                                                        "(produit_id,code_produit,nom,categorie,marque,prix_vente,prix_achat,poids_g,sku,statut) " +
                                                        "VALUES (:produit_id,:code_produit,:nom,:categorie,:marque,:prix_vente,:prix_achat,:poids_g,:sku,:statut)");
                                        rows.forEachRemaining(row -> {
                                            BoundStatementBuilder b = ps.boundStatementBuilder();
                                            setInt   (b, "produit_id",   row);
                                            setString(b, "code_produit", row);
                                            setString(b, "nom",          row);
                                            setString(b, "categorie",    row);
                                            setString(b, "marque",       row);
                                            setDouble(b, "prix_vente",   row);
                                            setDouble(b, "prix_achat",   row);
                                            setInt   (b, "poids_g",      row);
                                            setString(b, "sku",          row);
                                            setString(b, "statut",       row);
                                            s.execute(b.build());
                                        });
                                    }
                                })
                ).start();
    }

    public StreamingQuery writeStocks(Dataset<Row> df,
                                      String checkpointLocation) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        batch.filter(batch.col("produit_id").isNotNull())
                                .foreachPartition(rows -> {
                                    try (CqlSession s = openSession()) {
                                        PreparedStatement ps = s.prepare(
                                                "INSERT INTO stocks " +
                                                        "(produit_id,depot,stock_id,code_produit,quantite_disponible,quantite_reservee,seuil_reappro,statut) " +
                                                        "VALUES (:produit_id,:depot,:stock_id,:code_produit,:quantite_disponible,:quantite_reservee,:seuil_reappro,:statut)");
                                        rows.forEachRemaining(row -> {
                                            BoundStatementBuilder b = ps.boundStatementBuilder();
                                            setInt   (b, "produit_id",          row);
                                            setString(b, "depot",               row);
                                            setInt   (b, "stock_id",            row);
                                            setString(b, "code_produit",        row);
                                            setInt   (b, "quantite_disponible", row);
                                            setInt   (b, "quantite_reservee",   row);
                                            setInt   (b, "seuil_reappro",       row);
                                            setString(b, "statut",              row);
                                            s.execute(b.build());
                                        });
                                    }
                                })
                ).start();
    }

    public StreamingQuery writeRetours(Dataset<Row> df,
                                       String checkpointLocation) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        batch.filter(batch.col("client_id").isNotNull())
                                .foreachPartition(rows -> {
                                    try (CqlSession s = openSession()) {
                                        PreparedStatement ps = s.prepare(
                                                "INSERT INTO retours " +
                                                        "(client_id,retour_id,commande_ref,produit_id,date_retour,motif,statut,montant_rembourse,canal_achat) " +
                                                        "VALUES (:client_id,:retour_id,:commande_ref,:produit_id,:date_retour,:motif,:statut,:montant_rembourse,:canal_achat)");
                                        rows.forEachRemaining(row -> {
                                            BoundStatementBuilder b = ps.boundStatementBuilder();
                                            setIntS   (b, "client_id",        row);
                                            setIntS   (b, "retour_id",        row);
                                            setString (b, "commande_ref",     row);
                                            setIntS   (b, "produit_id",       row);
                                            setDateS  (b, "date_retour",      row);
                                            setString (b, "motif",            row);
                                            setString (b, "statut",           row);
                                            setDoubleS(b, "montant_rembourse",row);
                                            setString (b, "canal_achat",      row);
                                            s.execute(b.build());
                                        });
                                    }
                                })
                ).start();
    }

    public StreamingQuery writeVentes(Dataset<Row> df,
                                      String checkpointLocation) throws TimeoutException {
        return df.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        batch.filter(batch.col("canal").isNotNull())
                                .foreachPartition(rows -> {
                                    try (CqlSession s = openSession()) {
                                        PreparedStatement ps = s.prepare(
                                                "INSERT INTO ventes " +
                                                        "(canal,vente_id,reference,date_vente,client_id,produit_id,quantite," +
                                                        "prix_unitaire,remise,montant_ht,tva,montant_ttc,region,statut) " +
                                                        "VALUES (:canal,:vente_id,:reference,:date_vente,:client_id,:produit_id,:quantite," +
                                                        ":prix_unitaire,:remise,:montant_ht,:tva,:montant_ttc,:region,:statut)");
                                        rows.forEachRemaining(row -> {
                                            BoundStatementBuilder b = ps.boundStatementBuilder();
                                            setString(b, "canal",         row);
                                            setInt   (b, "vente_id",      row);
                                            setString(b, "reference",     row);
                                            setDate  (b, "date_vente",    row);
                                            setInt   (b, "client_id",     row);
                                            setInt   (b, "produit_id",    row);
                                            setInt   (b, "quantite",      row);
                                            setDouble(b, "prix_unitaire", row);
                                            setDouble(b, "remise",        row);
                                            setDouble(b, "montant_ht",    row);
                                            setDouble(b, "tva",           row);
                                            setDouble(b, "montant_ttc",   row);
                                            setString(b, "region",        row);
                                            setString(b, "statut",        row);
                                            s.execute(b.build());
                                        });
                                    }
                                })
                ).start();
    }
}