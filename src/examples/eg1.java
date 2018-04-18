package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

public class eg1{
    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect("panda");
        /*
        session.execute("USE demo;");
        session.execute("CREATE TABLE IF NOT EXISTS testTable (id varchar PRIMARY KEY , name varchar);");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1001' , 'Rooney' );");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1002' , 'Scholes' );");
        session.execute("INSERT INTO testTable (id , name ) VALUES ( '1003' , 'Falcao' );");
        */

        //ResultSet result=session.execute("select * from kangaroo.dm1");
        /*
        List<Row> rowList = result.all();
        System.out.println(rowList.size());
        */

        /*

        for (Row rows: result){
            if(result.getAvailableWithoutFetching() == 10 && !result.isFullyFetched()){
                result.fetchMoreResults();
            }
            System.out.println(rows.toString());
            //System.out.println(rows.getDecimal("day"));
        }
        */

        ResultSet result1=session.execute("select * from panda.dm1");

        List<Row> rowList = result1.all();
        System.out.println(rowList.size());

        session.close();
        cluster.close();
    }

}