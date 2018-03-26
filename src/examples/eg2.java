package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

public class eg2{
    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect("tfi");
        /*
        session.execute("USE demo;");
        session.execute("CREATE TABLE IF NOT EXISTS testTable (id varchar PRIMARY KEY , name varchar);");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1001' , 'Rooney' );");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1002' , 'Scholes' );");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1003' , 'Falcao' );");
        */

        session.execute("drop keyspace tfi");
        session.close();
        cluster.close();
    }

}