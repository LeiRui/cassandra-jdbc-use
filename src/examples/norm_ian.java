package examples;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import formulation.Column_ian;
import formulation.H_ian;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/*
  59 bytes a row
 */
public class norm_ian{

    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("norm_ian.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        String ks = "norm_ian";
        System.out.println(ks);
        System.out.println("");
        // columnspec
        double step1 = 1;
        List<Double> x1 = new ArrayList<Double>();
        for(int i = 50; i<=75; i=i+5) {
            x1.add((double)i);
        }
        List<Integer> y1 = new ArrayList<Integer>();
        y1.add(3);
        y1.add(5);
        y1.add(15);
        y1.add(5);
        y1.add(2);
        Column_ian ck1 = new Column_ian(step1, x1, y1);

        double step2 = 1;
        List<Double> x2 = new ArrayList<Double>();
        for(int i = 0; i<=1000; i++) {
            x2.add((double)i);
        }
        List<Integer> y2 = new ArrayList<Integer>();
        for(int i = 0; i<=999; i++) {
            y2.add(1);
        }
        Column_ian ck2 = new Column_ian(step2, x2, y2);

        double step3 = 1;
        List<Double> x3 = new ArrayList<Double>();
        for(int i = 0; i<=10000; i++) {
            x3.add((double)i);
        }
        List<Integer> y3 = new ArrayList<Integer>();
        for(int i = 0; i<=9999; i++) {
            y3.add(1);
        }
        Column_ian ck3 = new Column_ian(step3, x3, y3);

        List<Column_ian> CKdist = new ArrayList<Column_ian>();
        CKdist.add(ck1);
        CKdist.add(ck2);
        CKdist.add(ck3);

        // table schema definition (tables are already imported to cassandra)
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

        // 查询批次数
        int N = 20;

        // 暂固定查询范围及点参
        double qck1r1abs = 0;
        double qck1r2abs = 0.5;
        double qck1p1abs = 0.5;
        double qck1p2abs = 0.5;

        //for (int r = 0; r < 1; r++) {
        // columnspec
        System.out.println("cks dist: 略");

        //查询范围及点参
        System.out.println("qck1([" + qck1r1abs + "," + qck1r2abs + "]," + qck1p1abs + "," + qck1p2abs + ")");

        // qck1:
        double qck1r1 = qck1r1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_; // TODO double int
        double qck1r2 = qck1r2abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_;
        int qck1p1 = (int)Math.floor(qck1p1abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
        int qck1p2 = (int)Math.floor(qck1p2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
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

            // H公式
            H_ian h = new H_ian(996757,ckn, CKdist, 1, qck1r1, qck1r2,true, true,
                    new double[]{888, qck1p1, qck1p2},ackSeq[k]);
            System.out.print(", "+h.calculate()+", "+h.calculate(59));

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