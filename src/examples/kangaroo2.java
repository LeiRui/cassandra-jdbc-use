package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class kangaroo2{
    private static final Logger LOGGER = LoggerFactory.getLogger(kangaroo2.class);

    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    public static void main(String[] args) {
        PrintWriter pw=null;
        try {
            pw = new PrintWriter(new FileOutputStream("kangaroo.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();
        String ks = "kangaroo";

        List<String> cflist =new ArrayList();
        List<String> cfschemalist =new ArrayList();
        cflist.add("dm1");
        cfschemalist.add("ck1-ck2-ck3");
        cflist.add("dm2");
        cfschemalist.add("ck2-ck1-ck3");
        cflist.add("dm3");
        cfschemalist.add("ck2-ck3-ck1");
        cflist.add("dm4");
        cfschemalist.add("ck3-ck1-ck2");
        cflist.add("dm5");
        cfschemalist.add("ck1-ck3-ck2");
        cflist.add("dm6");
        cfschemalist.add("ck3-ck2-ck1");
        String s=",";
        for(int i = 0; i < cfschemalist.size(); i++) {
            s=s+cfschemalist.get(i)+",";
        }
        pw.write(s+"\n");

        // 查询批次数
        int N = 100;
        // 查询占比
        int []qck1perArray = new int[]{10,	9,	1,	8,	8,	8,	7,	7,	7,	7,	6,	6,	6,	6,	6,	5,	5,	5,	5,	5,	5,	4,	4,	4,	4,	4,	4,	4,	3,	3,	3,	3,	3,	3,	3,	3,	2,	2,	2,	2,	2,	2,	2,	2,	2,	1,	1,	1,	1,	1,	1,	1,	1,	1,	1,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0,	0};
        int []qck2perArray = new int[]{0,	1,	0,	2,	1,	0,	3,	2,	1,	0,	4,	3,	2,	1,	0,	5,	4,	3,	2,	1,	0,	6,	5,	4,	3,	2,	1,	0,	7,	6,	5,	4,	3,	2,	1,	0,	8,	7,	6,	5,	4,	3,	2,	1,	0,	9,	8,	7,	6,	5,	4,	3,	2,	1,	0,	10,	9,	8,	7,	6,	5,	4,	3,	2,	1,	0};
        int []qck3perArray = new int[]{0,	0,	9,	0,	1,	2,	0,	1,	2,	3,	0,	1,	2,	3,	4,	0,	1,	2,	3,	4,	5,	0,	1,	2,	3,	4,	5,	6,	0,	1,	2,	3,	4,	5,	6,	7,	0,	1,	2,	3,	4,	5,	6,	7,	8,	0,	1,	2,	3,	4,	5,	6,	7,	8,	9,	0,	1,	2,	3,	4,	5,	6,	7,	8,	9,	10};

        for (int r = 0; r < qck1perArray.length; r++) {
        //for (int r = 0; r < 1; r++) {
            // 查询占比
            int qck1per = qck1perArray[r];
            int qck2per = qck2perArray[r];
            int qck3per = qck3perArray[r];
            String perinfo = "qck1/"+qck1per+" qck2/"+qck2per+" qck3/"+qck3per+"";
            System.out.println(perinfo);
            LOGGER.info(perinfo);
            pw.write(perinfo+",");

            for(int k=0;k<cflist.size();k++) {
            //for(int k=cflist.size()-1;k>=0;k--) {
                String cf = cflist.get(k);
                //System.out.println(cf+"("+cfschemalist.get(k)+")");
                System.out.println(cf);
                System.out.println(cfschemalist.get(k));
                LOGGER.info(cf+":"+cfschemalist.get(k));


                // 范围及点查参数
                // qck1: year >=2014 and year <=2018 and month = 4 and day = 1
                int qck1r1 = 2014;
                int qck1r2 = 2018;
                int qck1p1 = 4;
                int qck1p2 = 1;
                String q1 = "select * from " + ks + "." + cf + " where year >= " + qck1r1 + " and year <= "
                        + qck1r2 + " and month = " + qck1p1 + " and day = " + qck1p2 + " allow filtering;";
                System.out.println("qck1: "+q1);
                LOGGER.info("qck1: "+q1);

                // qck2: year =2018 and month >= 4 and month<=8 and day = 15
                int qck2r1 = 4;
                int qck2r2 = 8;
                int qck2p1 = 2018;
                int qck2p2 = 15;
                String q2 = "select * from " + ks + "." + cf + " where year = " + qck2p1 + " and month >= "
                        + qck2r1 + " and month <= " + qck2r2 + " and day = " + qck2p2 + " allow filtering;";
                System.out.println("qck2: "+q2);
                LOGGER.info("qck2: "+q2);

                // qck3: year =2018 and month=9 and day >=10 and day<=16
                int qck3r1 = 10;
                int qck3r2 = 16;
                int qck3p1 = 2018;
                int qck3p2 = 9;
                String q3 = "select * from " + ks + "." + cf + " where year = " + qck3p1 + " and month = "
                        + qck3p2 + " and day >= " + qck3r1 + " and day <= " + qck3r2 + " allow filtering;";
                System.out.println("qck3: "+q3);
                LOGGER.info("qck3: "+q3);


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
                //System.out.println("");
                pw.write(""+costms+",");
                LOGGER.info(""+costms);
            }
            System.out.println("---------------next line----------------");
            pw.write("\n");
        }

        pw.close();
        session.close();
        cluster.close();
    }

}