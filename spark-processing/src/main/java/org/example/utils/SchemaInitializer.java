package org.example.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import org.example.config.CassandraConfig;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaInitializer {

    public static void initialize() {
        System.out.println(">>> Initialisation du schéma Cassandra...");

        // Étape 1 : créer le keyspace (session sans keyspace)
        try (CqlSession bootstrap = CassandraConfig.getBootstrapSession()) {
            bootstrap.execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + CassandraConfig.KEYSPACE +
                            " WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
            );
            System.out.println("    Keyspace " + CassandraConfig.KEYSPACE + " OK");
        }

        // Étape 2 : créer les tables (session avec keyspace)
        try (CqlSession session = CassandraConfig.getSession()) {

            InputStream is = SchemaInitializer.class
                    .getClassLoader()
                    .getResourceAsStream("schema.cql");
            if (is == null) {
                throw new RuntimeException("schema.cql introuvable dans le classpath");
            }

            String fullScript = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            List<String> statements = Arrays.stream(fullScript.split(";"))
                    .map(chunk -> Arrays.stream(chunk.split("\n"))
                            .filter(line -> !line.trim().startsWith("--"))
                            .collect(Collectors.joining("\n"))
                            .trim())
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            for (String stmt : statements) {
                String upper = stmt.toUpperCase();
                if (upper.startsWith("USE ") || upper.startsWith("CREATE KEYSPACE")) continue;
                try {
                    session.execute(stmt);
                    System.out.println("    Exécuté : " + stmt.split("\\n")[0]);
                } catch (Exception e) {
                    System.err.println("    ÉCHEC sur : " + stmt.split("\\n")[0]);
                    throw new RuntimeException("Erreur lors de l'exécution du statement", e);
                }
            }

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de l'initialisation du schéma", e);
        }

        System.out.println(">>> Schéma initialisé avec succès.");
    }
}