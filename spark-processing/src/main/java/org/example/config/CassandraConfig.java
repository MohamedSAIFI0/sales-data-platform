package org.example.config;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

public class CassandraConfig {

    private static final String HOST       = "127.0.0.1";
    private static final int    PORT       = 9042;
    public  static final String KEYSPACE   = "ventes_platform";
    private static final String DATACENTER = "datacenter1";

    /**
     * Session SANS keyspace — utilisée uniquement par SchemaInitializer
     * pour créer le keyspace et les tables.
     */
    public static CqlSession getBootstrapSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(HOST, PORT))
                .withLocalDatacenter(DATACENTER)
                .build();
    }

    /**
     * Session normale WITH keyspace — à appeler APRÈS SchemaInitializer.
     */
    public static CqlSession getSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(HOST, PORT))
                .withLocalDatacenter(DATACENTER)
                .withKeyspace(KEYSPACE)
                .build();
    }
}