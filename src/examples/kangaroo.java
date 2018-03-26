package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class kangaroo{
    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();
        String ks = "kangaroo";

        List<String> cflist =new ArrayList();
        cflist.add("dm1");
        cflist.add("dm2");
        cflist.add("dm3");

        for(int k=0;k<cflist.size();k++) {
            String cf = cflist.get(k);

            // 查询批次数
            int N = 100;
            // 查询占比
            int qck1per = 10;
            int qck2per = 0;
            int qck3per = 0;

            // 范围及点查参数
            // qck1: year >=2014 and year <=2018 and month = 4 and day = 1
            int qck1r1 = 2014;
            int qck1r2 = 2018;
            int qck1p1 = 4;
            int qck1p2 = 1;
            String q1 = "select * from " + ks + "." + cf + " where year >= " + qck1r1 + " and year <= "
                    + qck1r2 + " and month = " + qck1p1 + " and day = " + qck1p2 + " allow filtering;";
            System.out.println(q1);

            // qck2: year =2018 and month >= 4 and month<=8 and day = 15
            int qck2r1 = 4;
            int qck2r2 = 8;
            int qck2p1 = 2018;
            int qck2p2 = 15;
            String q2 = "select * from " + ks + "." + cf + " where year = " + qck2p1 + " and month >= "
                    + qck2r1 + " and month <= " + qck2r2 + " and day = " + qck2p2 + " allow filtering;";
            System.out.println(q2);

            // qck3: year =2018 and month=9 and day >=10 and day<=16
            int qck3r1 = 10;
            int qck3r2 = 16;
            int qck3p1 = 2018;
            int qck3p2 = 9;
            String q3 = "select * from " + ks + "." + cf + " where year = " + qck3p1 + " and month = "
                    + qck3p2 + " and day >= " + qck3r1 + " and day <= " + qck3r2 + " allow filtering;";
            System.out.println(q3);

            // warm up  25%
            for (int i = 0; i < N / 4; i++) {
                for (int j = 0; j < qck1per; j++) {
                    ResultSet rs = session.execute(q1);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
                for (int j = 0; j < qck2per; j++) {
                    ResultSet rs = session.execute(q2);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
                for (int j = 0; j < qck3per; j++) {
                    ResultSet rs = session.execute(q3);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
            }


            // 实证查询
            long elapsed = System.nanoTime();
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < qck1per; j++) {
                    ResultSet rs = session.execute(q1);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
                for (int j = 0; j < qck2per; j++) {
                    ResultSet rs = session.execute(q2);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
                for (int j = 0; j < qck3per; j++) {
                    ResultSet rs = session.execute(q3);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
            }
            elapsed = System.nanoTime() - elapsed;
            double costms = elapsed / (double) Math.pow(10, 6);
            System.out.println(costms);
        }


        session.close();
        cluster.close();
    }

}