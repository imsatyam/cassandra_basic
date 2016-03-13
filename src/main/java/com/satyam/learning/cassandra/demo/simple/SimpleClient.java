package com.satyam.learning.cassandra.demo.simple;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.satyam.learning.cassandra.demo.simple.util.CassandraConnector;

/**
 * Created by Satyam on 3/13/2016.
 */
public class SimpleClient {

    private final CassandraConnector connector = new CassandraConnector();

    public SimpleClient(String node, int port){
        connector.connect(node, port);
    }

    /**
     * Creates the simplex keyspace and two tables, songs and playlists.
     */
    public void createSchema() {
        connector.getSession().execute(
                "CREATE KEYSPACE simplex WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':3};");
        // create songs and playlist tables
        connector.getSession().execute(
                "CREATE TABLE simplex.songs (" +
                        "id uuid PRIMARY KEY," +
                        "title text," +
                        "album text," +
                        "artist text," +
                        "tags set<text>," +
                        "data blob" +
                        ");");

        System.out.println("Simplex keyspace and schema created.");
    }

    /**
     * Loads some data into the schema so that we can query the tables.
     */
    public void loadData() {
        // insert data in the tables
        connector.getSession().execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'," +
                        "{'jazz', '2013'})" +
                        ";");
        connector.getSession().execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "f6071e72-48ec-4fcb-bf3e-379c8a696488," +
                        "'Die Mösch'," +
                        "'In Gold'," +
                        "'Willi Ostermann'," +
                        "{'kölsch', '1996', 'birds'}" +
                        ");");
        connector.getSession().execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
                        "'Memo From Turner'," +
                        "'Performance'," +
                        "'Mick Jager'," +
                        "{'soundtrack', '1991'}" +
                        ");");

        System.out.println("Data loaded.");
    }

    /**
     * Queries the songs and playlists tables for data.
     */
    public void querySchema() {
        ResultSet results = connector.getSession().execute(
                "SELECT * FROM simplex.songs " +
                        "WHERE id = fbdf82ed-0063-4796-9c7c-a3d4f47b4b25;");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
                    row.getString("album"), row.getString("artist")));
        }
        System.out.println();
    }

    /**
     * Updates the songs table with a new song and then queries the table
     * to retrieve data.
     */
    public void updateSchema() {
        connector.getSession().execute(
                "UPDATE simplex.songs " +
                        "SET tags = tags + { 'entre-deux-guerres' } " +
                        "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        ResultSet results = connector.getSession().execute(
                "SELECT * FROM simplex.songs " +
                        "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        System.out.println(String.format("%-30s\t%-20s\t%-20s%-30s\n%s", "title", "album", "artist",
                "tags", "-------------------------------+-----------------------+--------------------+-------------------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
                    row.getString("album"),  row.getString("artist"), row.getSet("tags", String.class)));
        }
    }

    /**
     * Drops the specified schema.
     * @param keyspace the keyspace to drop (and all of its data)
     */
    public void dropSchema(String keyspace) {
        connector.getSession().execute("DROP KEYSPACE " + keyspace);
        System.out.println("Finished dropping " + keyspace + " keyspace.");
    }

    /**
     * Shuts down the session and its cluster.
     */
    public void close() {
        connector.getSession().close();
        connector.getSession().getCluster().close();
    }

    /**
     * Creates  simple client application that illustrates connecting to
     * a Cassandra cluster. retrieving metadata, creating a schema,
     * loading data into it, and then querying it.
     * @param args ignored
     */
    public static void main(String[] args) {
        SimpleClient client = new SimpleClient("localhost", 9042);
        client.createSchema();
        client.loadData();
        client.querySchema();
        client.updateSchema();
        client.dropSchema("simplex");
        client.close();
    }



}
