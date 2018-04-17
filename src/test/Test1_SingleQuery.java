package test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import formulation.Column_ian;
import formulation.H_ian;
import formulation.HwithoutPrefix;
import jnr.ffi.annotations.In;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class Test1_SingleQuery {

    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;
    private static int rowSize = 24;

    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("test1-SingleQuery.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        String ks = "panda";
        System.out.println(ks);
        // columnspec
        System.out.println("ck1 dist:U[1,100], ck2 dist:U[1,100], ck3 dist:U[1,100]");
        double step = 1;
        List<Double> x = new ArrayList<Double>();
        for(int i = 1; i<=101; i++) {
            x.add((double)i);
        }
        List<Integer> y = new ArrayList<Integer>();
        for(int i = 1; i<=100; i++) {
            y.add(1);
        }
        Column_ian ck1 = new Column_ian(step, x, y);
        Column_ian ck2 = new Column_ian(step, x, y);
        Column_ian ck3 = new Column_ian(step, x, y);
        List<Column_ian> CKdist = new ArrayList<Column_ian>();
        CKdist.add(ck1);
        CKdist.add(ck2);
        CKdist.add(ck3);



        // table schema definition (tables are already created in cassandra)
        List<String> cflist = new ArrayList();
        List<String> cfschemalist = new ArrayList();
        int[][] ackSeq = new int[6][];
        cflist.add("dm1");
        cfschemalist.add("ck1-ck2-ck3");
        ackSeq[0]=new int[]{1,2,3};
        cflist.add("dm2");
        cfschemalist.add("ck1-ck3-ck2");
        ackSeq[1]=new int[]{1,3,2};
        cflist.add("dm3");
        cfschemalist.add("ck2-ck1-ck3");
        ackSeq[2]=new int[]{2,1,3};
        cflist.add("dm4");
        cfschemalist.add("ck3-ck1-ck2");
        ackSeq[3]=new int[]{3,1,2};
        cflist.add("dm5");
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

        // 范围查询参数
        double qck1r1abs = 0;
        double qck1r2abs = 0.1;
        double qck1p1abs = 0.5;
        double qck1p2abs = 0.5;
        System.out.print("qck1([" + qck1r1abs + "," + qck1r2abs + "]," + qck1p1abs + "," + qck1p2abs + ")");
        int qck1r1 = (int)Math.floor(qck1r1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_); // TODO double int
        int qck1r2 = (int)Math.floor(qck1r2abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_);
        int qck1p1 = (int)Math.floor(qck1p1abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
        int qck1p2 = (int)Math.floor(qck1p2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
        String q1_format = "select * from " + ks + ".%s"
                + " where pkey=1 and ck1 >= " + qck1r1 + " and ck1 <= " + qck1r2 // TODO
                + " and ck2 = " + qck1p1
                + " and ck3 = " + qck1p2
                + " allow filtering;";
        System.out.println(":" + q1_format);

        // 查询批次数
        int N = 100;

        // 控制变量：compare different data models(different ACKSets)
        for (int k = 0; k < cflist.size(); k++) {
            String cf = cflist.get(k);
            System.out.print(cf);
            System.out.print(", " + cfschemalist.get(k));
            String q1 = String.format(q1_format, cf);// 代入cf，构造查询语句

            // H模型代价计算
            H_ian h = new H_ian(1000000,ckn, CKdist, 1, qck1r1, qck1r2,true, true,
                    new double[]{888, qck1p1, qck1p2},ackSeq[k]);
            //System.out.print(", "+h.calculate()+", "+h.calculate(rowSize));
            System.out.print(String.format(", HR:%6d, HB:%3d", Math.round(h.calculate()),h.calculate(rowSize)));

            // 实测代价
            // warm up  25%
            for (int i = 0; i < 20; i++) {
                ResultSet rs = session.execute(q1);
                int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
            }

            // 实证查询
            List<Double> resRecord = new ArrayList<Double>();
            double sumup = 0;
            for (int m = 0; m < N; m++) {
                long elapsed = System.nanoTime();
                ResultSet rs = session.execute(q1);
                int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                elapsed = System.nanoTime() - elapsed;
                double cost = elapsed / (double) Math.pow(10, 6); // unit: ms
                resRecord.add(cost);
                sumup += cost;
            }
            sumup /= N;
            System.out.print(String.format(", Real-Mean:%8.3f", sumup));
            pw.write("" + sumup + ","); // 平均值

            // 统计min,80th percentile,95th percentile,max
            Collections.sort(resRecord);
            int eighty_index = (int) Math.ceil(N * 0.8);
            int ninety_five_index = (int) Math.ceil(N * 0.95);
            System.out.println(String.format(", min:%8.3f, 80th percentile:%8.3f, 95th percentile:%8.3f, max:%8.3f"
                    ,resRecord.get(0)
                    ,resRecord.get(eighty_index - 1)
                    ,resRecord.get(ninety_five_index - 1)
                    ,resRecord.get(resRecord.size() - 1)));
            /*
            pw_real_detail.write("" + sumup + "," + resRecord.get(0) + "," + resRecord.get(eighty_index - 1)
                    + "," + resRecord.get(ninety_five_index - 1) + "," + resRecord.get(resRecord.size() - 1)
                    + ",,");
                    */
        }
        System.out.println("---------------next line----------------");
        pw.write("\n");

        pw.close();
        session.close();
        cluster.close();
    }

}