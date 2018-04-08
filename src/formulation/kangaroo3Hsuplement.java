package formulation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/*
    ck1 year dist: U[1951,2050], ck2 month dist: U[1,12], ck3 day dist: U[1,30]
    qck1([0.4,0.6],0.3,0.3), qck2(0.6,[0.3,0.6],0.3), qck3(0.4,0.6,[0.3,0.6])
    :select * from kangaroo.%s where year >= 1991 and year <= 2010 and month = 4 and day = 10 allow filtering;
    :select * from kangaroo.%s where year = 2010 and month >= 4 and month <= 8 and day = 10 allow filtering;
    :select * from kangaroo.%s where year = 1991 and month = 8 and day >= 10 and day <= 18 allow filtering;
    qck1/10 qck2/0 qck3/0
    dm1, ck1-ck2-ck3, 6469.040416
    dm2, ck2-ck1-ck3, 4623.36171
    dm3, ck2-ck3-ck1, 4387.089069
    dm4, ck3-ck1-ck2, 4176.620641
    dm5, ck1-ck3-ck2, 6246.382451
    dm6, ck3-ck2-ck1, 3740.036788
    ---------------next line----------------
    ......
 */
public class kangaroo3Hsuplement {

    //public static Cluster cluster;
    //public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("kangaroo_v3_H_suplement.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        //Session session = cluster.connect();

        String ks = "kangaroo";
        /*
        // columnspec
        // ck1 U[a,b]
        int dis_a1 = 1951;
        int dis_b1 = 2050;
        // ck2 U[a,b]
        int dis_a2 = 1;
        int dis_b2 = 12;
        // ck3 U[a,b]
        int dis_a3 = 1;
        int dis_b3 = 30;
        */
        int [] dis_a = new int[]{1951,1,1};
        int [] dis_b = new int[]{2050,12,30};

        List<List> kangaroo_dist = new ArrayList<List>();
        for(int i=0;i<ckn;i++) {
            List colDist = new ArrayList();
            colDist.add(true);
            colDist.add(dis_a[i]);
            colDist.add(dis_b[i]);
            kangaroo_dist.add(colDist);
        }
        int dis_a1 = dis_a[0];
        int dis_b1 = dis_b[0];
        int dis_a2 = dis_a[1];
        int dis_b2 = dis_b[1];
        int dis_a3 = dis_a[2];
        int dis_b3 = dis_b[2];

        // table schema definition (tables are already imported to cassandra)
        List<String> cflist = new ArrayList();
        List<String> cfschemalist = new ArrayList();
        int[][] ackSeq = new int[6][];
        cflist.add("dm1");
        cfschemalist.add("ck1-ck2-ck3");
        ackSeq[0]=new int[]{1,2,3};
        cflist.add("dm2");
        cfschemalist.add("ck2-ck1-ck3");
        ackSeq[1]=new int[]{2,1,3};
        cflist.add("dm3");
        cfschemalist.add("ck2-ck3-ck1");
        ackSeq[2]=new int[]{2,3,1};
        cflist.add("dm4");
        cfschemalist.add("ck3-ck1-ck2");
        ackSeq[3]=new int[]{3,1,2};
        cflist.add("dm5");
        cfschemalist.add("ck1-ck3-ck2");
        ackSeq[4]=new int[]{1,3,2};
        cflist.add("dm6");
        cfschemalist.add("ck3-ck2-ck1");
        ackSeq[5]=new int[]{3,2,1};
        // write header
        String s = "qck1per,qck2per,qck3per,";
        for (int i = 0; i < cfschemalist.size(); i++) {
            s = s + cfschemalist.get(i) + ",";
        }
        pw.write(s + "\n");


        // 查询批次数
        int N = 100;
        // 查询占比
        int[] qck1perArray = new int[]{10};//{10, 9, 9, 8, 8, 8, 7, 7, 7, 7, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int[] qck2perArray = new int[]{0};//{0, 1, 0, 2, 1, 0, 3, 2, 1, 0, 4, 3, 2, 1, 0, 5, 4, 3, 2, 1, 0, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        int[] qck3perArray = new int[]{0};//{0, 0, 1, 0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 8, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        // 暂固定查询范围及点参
        double qck1r1abs = 0.9;
        double qck1r2abs = 1;
        double qck1p1abs = 0.8;
        double qck1p2abs = 0.8;

        double qck2r1abs = 0.0;
        double qck2r2abs = 0.2;
        double qck2p1abs = 0.8;
        double qck2p2abs = 0.9;

        double qck3r1abs = 0.3;
        double qck3r2abs = 0.6;
        double qck3p1abs = 0.4;
        double qck3p2abs = 0.6;

        for (int r = 0; r < qck1perArray.length; r++) { // 控制变量：改变查询占比
            //for (int r = 0; r < 1; r++) {
            // columnspec
            System.out.println("ck1 year dist: U[" + dis_a1 + "," + dis_b1 + "], ck2 month dist: U[" + dis_a2 + "," + dis_b2 + "], ck3 day dist: U["
                    + dis_a3 + "," + dis_b3 + "]");

            //查询范围及点参
            System.out.println("qck1(["+qck1r1abs+","+qck1r2abs+"],"+qck1p1abs+","+qck1p2abs+"), "
                    + "qck2("+qck2p1abs+",["+qck2r1abs+","+qck2r2abs+"]"+","+qck2p2abs+"), "
                    + "qck3("+qck3p1abs+","+qck3p2abs+",["+qck3r1abs+","+qck3r2abs+"])");

            // qck1: year >= 40% ? and year <= 60% ? and month = 30% ? and day = 30% ?
            int qck1r1 = (int) Math.round(qck1r1abs * (dis_b1 - dis_a1) + dis_a1);
            int qck1r2 = (int) Math.round(qck1r2abs * (dis_b1 - dis_a1) + dis_a1);
            int qck1p1 = (int) Math.round(qck1p1abs * (dis_b2 - dis_a2) + dis_a2);
            int qck1p2 = (int) Math.round(qck1p2abs * (dis_b3 - dis_a3) + dis_a3);
            String q1_format = "select * from " + ks + ".%s"
                    + " where year >= " + qck1r1 + " and year <= " + qck1r2
                    + " and month = " + qck1p1
                    + " and day = " + qck1p2
                    + " allow filtering;";
            // qck2: year =60% ? and month >= 30% ? and month<=60% ? and day = 30% ?
            int qck2r1 = (int) Math.round(qck2r1abs * (dis_b2 - dis_a2) + dis_a2);
            int qck2r2 = (int) Math.round(qck2r2abs * (dis_b2 - dis_a2) + dis_a2);
            int qck2p1 = (int) Math.round(qck2p1abs * (dis_b1 - dis_a1) + dis_a1);
            int qck2p2 = (int) Math.round(qck2p2abs * (dis_b3 - dis_a3) + dis_a3);
            String q2_format = "select * from " + ks + ".%s"
                    + " where year = " + qck2p1
                    + " and month >= " + qck2r1 + " and month <= " + qck2r2
                    + " and day = " + qck2p2
                    + " allow filtering;";
            // qkc3: year =40% ?  and month=60% ? and day >=30% ? and day<=60% ?
            int qck3r1 = (int) Math.round(qck3r1abs * (dis_b3 - dis_a3) + dis_a3);
            int qck3r2 = (int) Math.round(qck3r2abs * (dis_b3 - dis_a3) + dis_a3);
            int qck3p1 = (int) Math.round(qck3p1abs * (dis_b1 - dis_a1) + dis_a1);
            int qck3p2 = (int) Math.round(qck3p2abs * (dis_b2 - dis_a2) + dis_a2);
            String q3_format = "select * from " + ks + ".%s"
                    + " where year = " + qck3p1
                    + " and month = " + qck3p2
                    + " and day >= " + qck3r1 + " and day <= " + qck3r2
                    + " allow filtering;";

            System.out.println(":"+q1_format);
            System.out.println(":"+q2_format);
            System.out.println(":"+q3_format);

            //查询占比:控制变量
            int qck1per = qck1perArray[r];
            int qck2per = qck2perArray[r];
            int qck3per = qck3perArray[r];
            System.out.println("qck1/"+qck1per+" qck2/"+qck2per+" qck3/"+qck3per+" N="+N);
            pw.write("" + qck1per + "," + qck2per + "," + qck3per + ",");

            for (int k = 0; k < cflist.size(); k++) { // 控制变量：compare different data models(different ACKSets)
                //for(int k=cflist.size()-1;k>=0;k--) {
                String cf = cflist.get(k);
                System.out.print(cf);
                System.out.print(", " + cfschemalist.get(k));

                // 代入cf，构造查询语句
                String q1 = String.format(q1_format, cf);
                String q2 = String.format(q2_format, cf);
                String q3 = String.format(q3_format, cf);

                // H公式代价计算
                //H(int PN, int ckn, List<List> CKdist, int qckn, double qck_r1, double qck_r2, double[] qck_p, int[] ackSeq){
                H h1 = new H(36000,3,kangaroo_dist, 1,qck1r1abs,qck1r2abs,new double[]{888,qck1p1abs,qck1p2abs},ackSeq[k]);
                H h2 = new H(36000,3,kangaroo_dist, 2,qck2r1abs,qck2r2abs,new double[]{qck2p1abs,888,qck2p2abs},ackSeq[k]);
                H h3 = new H(36000,3,kangaroo_dist, 3,qck3r1abs,qck3r2abs,new double[]{qck3p1abs,qck3p2abs,888},ackSeq[k]);
                h1.calculate();
                h2.calculate();
                h3.calculate();
                double res = h1.resP*qck1per+h2.resP*qck2per+h3.resP*qck3per;
                System.out.println(", " + res);
                pw.write("" + res + ",");
                /*
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
                System.out.println(", " + costms);
                //System.out.println("");
                pw.write("" + costms + ",");
                */

            }
            System.out.println("---------------next line----------------");
            pw.write("\n");
        }

        pw.close();
        //session.close();
        //cluster.close();
    }

}