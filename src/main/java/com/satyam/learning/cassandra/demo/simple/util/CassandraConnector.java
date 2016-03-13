package com.satyam.learning.cassandra.demo.simple.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import static java.lang.System.out;

/**
 * Created by Satyam on 3/13/2016.
 * Contains the logic to connect to cassandra. It is a basic example.
 * It can later be replaced by Spring configuration and Netflix astyanax.
 */
public class CassandraConnector {

    /** Cassandra Cluster. */
    private Cluster cluster;

    /** Cassandra Session. */
    private Session session;

    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * address and port number.
     *
     * @param node Cluster node IP address.
     * @param port Port of cluster host.
     */
    public void connect(final String node, final int port)
    {
        this.cluster = Cluster.builder()
                        .addContactPoint(node)
                        .withPort(port)
                        .withClusterName("Test Cluster")
                        .build();
        final Metadata metadata = cluster.getMetadata();
        out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts())
        {
            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    /**
     * Provide my Session.
     *
     * @return My session.
     */
    public Session getSession()
    {
        return this.session;
    }

    /** Close cluster. */
    public void close()
    {
        cluster.close();
    }

}
