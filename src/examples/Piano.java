package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/*
 small data test only 1000 rows
 */
public class Piano {

    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("rabbit.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        String ks = "piano";
        // columnspec
        // ck1 U[a,b]
        int dis_a1 = 1;
        int dis_b1 = 10;
        // ck2 U[a,b]
        int dis_a2 = 1;
        int dis_b2 = 10;
        // ck3 U[a,b]
        int dis_a3 = 1;
        int dis_b3 = 10;
        //System.out.println("ck1 year dist: U[" + dis_a1 + "," + dis_b1 + "], ck2 month dist: U[" + dis_a2 + "," + dis_b2 + "], ck3 day dist: U["
        //+ dis_a3 + "," + dis_b3 + "]");
        //System.out.println("");

        // table schema definition (tables are already imported to cassandra)
        List<String> cflist = new ArrayList();
        List<String> cfschemalist = new ArrayList();
        cflist.add("dm1");
        cfschemalist.add("ck1-ck2-ck3");
        cflist.add("dm2");
        cfschemalist.add("ck1-ck3-ck2");
        cflist.add("dm3");
        cfschemalist.add("ck2-ck1-ck3");
        cflist.add("dm4");
        cfschemalist.add("ck3-ck1-ck2");
        cflist.add("dm5");
        cfschemalist.add("ck2-ck3-ck1");
        cflist.add("dm6");
        cfschemalist.add("ck3-ck2-ck1");
        // write header
        String s = "";
        for (int i = 0; i < cfschemalist.size(); i++) {
            s = s + cfschemalist.get(i) + ",";
        }
        pw.write(s + "\n");

        // 查询批次数
        int N = 20;

        // 暂固定查询范围及点参
        double qck1r1abs = 0.3;
        double qck1r2abs = 0.7;
        double qck1p1abs = 0.5;
        double qck1p2abs = 0.5;

        //for (int r = 0; r < 1; r++) {
        // columnspec
        System.out.println("ck1 dist: U[" + dis_a1 + "," + dis_b1 + "], ck2 dist: U[" + dis_a2 + "," + dis_b2 + "], ck3 dist: U["
                + dis_a3 + "," + dis_b3 + "]");

        //查询范围及点参
        System.out.println("qck1([" + qck1r1abs + "," + qck1r2abs + "]," + qck1p1abs + "," + qck1p2abs + ")");

        // qck1: year >= 40% ? and year <= 60% ? and month = 30% ? and day = 30% ?
        int qck1r1 = (int) Math.round(qck1r1abs * (dis_b1 - dis_a1) + dis_a1);
        int qck1r2 = (int) Math.round(qck1r2abs * (dis_b1 - dis_a1) + dis_a1);
        int qck1p1 = (int) Math.round(qck1p1abs * (dis_b2 - dis_a2) + dis_a2);
        int qck1p2 = (int) Math.round(qck1p2abs * (dis_b3 - dis_a3) + dis_a3);
        String q1_format = "select * from " + ks + ".%s"
                + " where pkey=1 and ck1 >= " + qck1r1 + " and ck1 <= " + qck1r2
                + " and ck2 = " + qck1p1
                + " and ck3 = " + qck1p2
                + " allow filtering;";
        System.out.println(":" + q1_format);

        for (int k = 0; k < cflist.size(); k++) { // 控制变量：compare different data models(different ACKSets)
            //for(int k=cflist.size()-1;k>=0;k--) {
            String cf = cflist.get(k);
            System.out.print(cf);
            System.out.print(", " + cfschemalist.get(k));

            // 代入cf，构造查询语句
            String q1 = String.format(q1_format, cf);

            // warm up  25%
            for (int i = 0; i < 20; i++) {
                ResultSet rs = session.execute(q1);
                int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
            }

            // 实证查询
            double[] instance = new double[5];
            double sumup=0;
            for (int m = 0; m < 5; m++) {
                long elapsed = System.nanoTime();
                for (int i = 0; i < N; i++) {
                    ResultSet rs = session.execute(q1);
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
                elapsed = System.nanoTime() - elapsed;
                instance[m] = (elapsed / (double) Math.pow(10, 6)) / N; // average; unit: ms
                sumup+=instance[m];
            }
            sumup /= 5;
            System.out.print(", " + sumup+": ");
            pw.write("" + sumup + ",");
            for(int m=0;m<5;m++){
                System.out.print(instance[m]+", ");
            }
            System.out.println("");
        }
        System.out.println("---------------next line----------------");
        pw.write("\n");

        pw.close();
        session.close();
        cluster.close();
    }

}