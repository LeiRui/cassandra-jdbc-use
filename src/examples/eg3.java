package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

public class eg3{
    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        /*
        session.execute("USE demo;");
        session.execute("CREATE TABLE IF NOT EXISTS testTable (id varchar PRIMARY KEY , name varchar);");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1001' , 'Rooney' );");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1002' , 'Scholes' );");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1003' , 'Falcao' );");
        */

        session.execute("CREATE KEYSPACE cycling WITH replication " +
                "= {'class': 'SimpleStrategy', 'replication_factor': 1};");
        session.execute("CREATE TABLE cycling.cyclist_name ("+
                "id UUID PRIMARY KEY,"+
                "lastname text,"+
                "firstname text);");
        session.execute("INSERT INTO cycling.cyclist_name (id, lastname, firstname) " +
                "   VALUES (5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 'VOS','Marianne');");
        session.execute("COPY cycling.cyclist_name (id,lastname) " +
                "TO 'cyclist_lastname.csv' WITH HEADER = TRUE ;");
        session.close();
        cluster.close();
    }

}