package com.madhouse.madmax.utils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CassandraClient {
    private String[] contact_points = {"127.0.0.1"};
    private int port = 9042;
    private String keySpace = "w";
    private String table = "tt";
    private Cluster cluster;
    private Session session;
    private PreparedStatement prepareStatement;

    private  String GET_SQL = "select tag from " + keySpace + "." + table + " where did = ? limit 1;";

    public CassandraClient(String[] contactPoints) {
        contact_points = contactPoints;
        connect(contact_points, port);
    }

    public CassandraClient(String[] contactPoints, int p) {
        contact_points = contactPoints;
        port = p;
        connect(contact_points, port);
    }

    public CassandraClient(String[] contactPoints, String ks, String t) {
        contact_points = contactPoints;
        keySpace = ks;
        table = t;
        connect(contact_points, port);
    }

    private void connect(String[] contactPoints, int port) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
        /*poolingOptions
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);*/
        //poolingOptions.setNewConnectionThreshold(HostDistance.LOCAL,32);


        cluster = Cluster.builder().addContactPoints(contactPoints)
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withoutMetrics()
                .withPoolingOptions(poolingOptions).withPort(port).build();
        System.out.printf("Connected to cluster: %s\n", cluster.getMetadata().getClusterName());
        //PoolingOptions p = cluster.getConfiguration().getPoolingOptions();
        for (Host host : cluster.getMetadata().getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        cluster.init();
        session = cluster.connect();
        prepareStatement = session.prepare(GET_SQL);
    }

    public void monitor(){
        final LoadBalancingPolicy loadBalancingPolicy =
                cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
        final PoolingOptions poolingOptions =
                cluster.getConfiguration().getPoolingOptions();

        ScheduledExecutorService scheduled =
                Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(new Runnable() {
            public void run() {
                Session.State state = session.getState();
                for (Host host : state.getConnectedHosts()) {
                    HostDistance distance = loadBalancingPolicy.distance(host);
                    int connections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    System.out.printf("%s connections=%d, current load=%d, maxload=%d%n",
                    host, connections, inFlightQueries,
                            connections *
                                    poolingOptions.getMaxRequestsPerConnection(distance));
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public ResultSet query(String id) {
        //String sql = "select tag from " + keySpace + "." + table + " where did = '" + id + "';";
        //System.out.println("#####id = " + id);
        BoundStatement bindStatement = new BoundStatement(prepareStatement).bind(id);
        try {
            return session.execute(bindStatement);
        } catch (Exception e) {
            System.err.println(e.toString());
        }
        return null;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    /*public static void main(String[] args) {

        CreateAndPopulateKeyspace client = new CreateAndPopulateKeyspace();

        try {

            client.connect(CONTACT_POINTS, PORT);
            client.createSchema();
            client.loadData();
            client.querySchema();

        } finally {
            client.close();
        }
    }*/


    /**
     * Creates the schema (keyspace) and tables for this example.
     */
    public void createSchema() {

        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
                        + "= {'class':'SimpleStrategy', 'replication_factor':1};");

        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.songs ("
                        + "id uuid PRIMARY KEY,"
                        + "title text,"
                        + "album text,"
                        + "artist text,"
                        + "tags set<text>,"
                        + "data blob"
                        + ");");

        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.playlists ("
                        + "id uuid,"
                        + "title text,"
                        + "album text, "
                        + "artist text,"
                        + "song_id uuid,"
                        + "PRIMARY KEY (id, title, album, artist)"
                        + ");");
    }

    /**
     * Inserts data into the tables.
     */
    public void loadData() {

        session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) "
                        + "VALUES ("
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'La Petite Tonkinoise',"
                        + "'Bye Bye Blackbird',"
                        + "'Joséphine Baker',"
                        + "{'jazz', '2013'})"
                        + ";");

        session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) "
                        + "VALUES ("
                        + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
                        + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                        + "'La Petite Tonkinoise',"
                        + "'Bye Bye Blackbird',"
                        + "'Joséphine Baker'"
                        + ");");
    }
}
