package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import formulation.HwithoutPrefix;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
 forget about the year-month-day
 */
public class rabbit {

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

        String ks = "rabbit";
        // columnspec
        // ck1 U[a,b]
        int dis_a1 = 1;
        int dis_b1 = 100;
        // ck2 U[a,b]
        int dis_a2 = 1;
        int dis_b2 = 100;
        // ck3 U[a,b]
        int dis_a3 = 1;
        int dis_b3 = 100;

        List<List> data_dist = new ArrayList<List>();
        List colDist1 = new ArrayList();
        colDist1.add(true);
        colDist1.add(1.0); // 离散步长
        colDist1.add(dis_a1);
        colDist1.add(dis_b1);
        data_dist.add(colDist1);
        List colDist2 = new ArrayList();
        colDist2.add(true);
        colDist1.add(1.0); // 离散步长
        colDist2.add(dis_a2);
        colDist2.add(dis_b2);
        data_dist.add(colDist2);
        List colDist3 = new ArrayList();
        colDist3.add(true);
        colDist1.add(1.0); // 离散步长
        colDist3.add(dis_a3);
        colDist3.add(dis_b3);
        data_dist.add(colDist3);
        //System.out.println("ck1 year dist: U[" + dis_a1 + "," + dis_b1 + "], ck2 month dist: U[" + dis_a2 + "," + dis_b2 + "], ck3 day dist: U["
        //+ dis_a3 + "," + dis_b3 + "]");
        //System.out.println("");

        // table schema definition (tables are already imported to cassandra)
        List<String> cflist = new ArrayList();
        List<String> cfschemalist = new ArrayList();
        int[][] ackSeq = new int[6][];
        cflist.add("dm1");
        cfschemalist.add("ck1-ck2-ck3");
        ackSeq[0]=new int[]{1,2,3};
        cflist.add("dm5");
        cfschemalist.add("ck1-ck3-ck2");
        ackSeq[1]=new int[]{1,3,2};
        cflist.add("dm2");
        cfschemalist.add("ck2-ck1-ck3");
        ackSeq[2]=new int[]{2,1,3};
        cflist.add("dm4");
        cfschemalist.add("ck3-ck1-ck2");
        ackSeq[3]=new int[]{3,1,2};
        cflist.add("dm3");
        cfschemalist.add("ck2-ck3-ck1");
        ackSeq[4]=new int[]{2,3,1};
        cflist.add("dm6");
        cfschemalist.add("ck3-ck2-ck1");
        ackSeq[5]=new int[]{3,2,1};
        // write header
        String s = "";
        for (int i = 0; i < cfschemalist.size(); i++) {
            s = s + cfschemalist.get(i) + ",";
        }
        pw.write(s + "\n");

        // 查询批次数
        int N = 20;

        // 暂固定查询范围及点参
        double qck1r1abs = 0.9;
        double qck1r2abs = 1;
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
                //+ " where pkey=1 and ck1 >= " + qck1r1 + " and ck1 <= " + qck1r2
                + " where pkey=1 and ck1 = " + qck1r1
                + " and ck2 = " + qck1p1
                + " and ck3 = " + qck1p2
                + " allow filtering;";
        System.out.println(":" + q1_format);

        for (int k = 0; k < cflist.size(); k++) { // 控制变量：compare different data models(different ACKSets)
            //for(int k=cflist.size()-1;k>=0;k--) {
            //if(k==0||k==1)
              //  continue;
            String cf = cflist.get(k);
            System.out.print(cf);
            System.out.print(", " + cfschemalist.get(k));

            // H公式
            HwithoutPrefix h = new HwithoutPrefix(24,65536,1000000,3,data_dist, 1,qck1r1abs,qck1r2abs,new double[]{888,qck1p1abs,qck1p2abs},ackSeq[k]);
            System.out.print(", "+h.Cost(1));

            // 代入cf，构造查询语句
            String q1 = String.format(q1_format, cf);

            // warm up  25%
            for (int i = 0; i < 20; i++) {
                ResultSet rs = session.execute(q1);
                //int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                Iterator<Row> iter = rs.iterator();
                System.out.println("getAvailableWithoutFetching: "+rs.getAvailableWithoutFetching());
                while(iter.hasNext()) {
                    if(rs.getAvailableWithoutFetching()==100 && !rs.isFullyFetched()) {
                        rs.fetchMoreResults();
                        System.out.println("yes:getAvailableWithoutFetching: "+rs.getAvailableWithoutFetching());
                    }
                    Row row = iter.next();
                    //System.out.println(row);
                }
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