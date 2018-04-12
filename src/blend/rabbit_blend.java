package blend;

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
import java.util.Iterator;
import java.util.List;


public class rabbit_blend {

    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    private static int rowSize =24;

    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("rabbit_blend.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        String ks = "rabbit";
        System.out.println(ks);
        System.out.println("");
        // columnspec
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
        Column_ian ck4 = new Column_ian(step, x, y);
        Column_ian ck5 = new Column_ian(step, x, y);
        Column_ian ck6 = new Column_ian(step, x, y);
        List<Column_ian> CKdist = new ArrayList<Column_ian>();
        CKdist.add(ck1);
        CKdist.add(ck2);
        CKdist.add(ck3);
        CKdist.add(ck4);
        CKdist.add(ck5);
        CKdist.add(ck6);


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
        String s = "qck1per,qck2per,qck3per,";
        for (int i = 0; i < cfschemalist.size(); i++) {
            s = s + cfschemalist.get(i) + ","; // 实际代价的表头
        }
        pw.write(s);
        s= ",";
        for (int i = 0; i < cfschemalist.size(); i++) {
            s = s + "HB:"+cfschemalist.get(i) + ","; // 公式代价blocks的表头
        }
        pw.write(s);
        s= ",";
        for (int i = 0; i < cfschemalist.size(); i++) {
            s = s + "HR:"+cfschemalist.get(i) + ","; // 公式代价rows的表头
        }
        pw.write(s);
        pw.write(", chooseR,chooseH,error((H-R)/R)%)\n");

        // 查询批次数
        int N = 10;

        // 查询占比
        int[] qck1perArray = new int[]{10, 9};//, 9, 8, 8, 8, 7, 7, 7, 7, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int[] qck2perArray = new int[]{0, 1};//, 0, 2, 1, 0, 3, 2, 1, 0, 4, 3, 2, 1, 0, 5, 4, 3, 2, 1, 0, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        int[] qck3perArray = new int[]{0, 0};//, 1, 0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 8, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // 暂固定查询范围及点参
        double qck1r1abs = 0;
        double qck1r2abs = 0.1;
        double qck1p1abs = 0.5;
        double qck1p2abs = 0.5;

        double qck2r1abs = 0;
        double qck2r2abs = 0.1;
        double qck2p1abs = 0.5;
        double qck2p2abs = 0.5;

        double qck3r1abs = 0;
        double qck3r2abs = 0.1;
        double qck3p1abs = 0.5;
        double qck3p2abs = 0.5;

        //for (int r = 0; r < 1; r++) {
        // columnspec
        System.out.println("cks dist: 同分布，step=1, ck1:U[1,100], ck2:U[1,100], ck3:U[1,100]");

        //查询范围及点参
        System.out.println("qck1([" + qck1r1abs + "," + qck1r2abs + "]," + qck1p1abs + "," + qck1p2abs + ")");
        System.out.println("qck2("+qck2p1abs+",["+qck2r1abs+","+qck2r2abs+"],"+qck2p2abs+")");
        System.out.println("qck3("+qck3p1abs+","+qck3p2abs+",["+qck3r1abs+","+qck3r2abs+"])");

        // qck
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

        int qck2r1 = (int)Math.floor(qck1r1abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_); // TODO double int
        int qck2r2 = (int)Math.floor(qck1r2abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
        int qck2p1 = (int)Math.floor(qck1p1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_);
        int qck2p2 = (int)Math.floor(qck1p2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
        String q2_format = "select * from " + ks + ".%s"
                + " where pkey=1 and ck2 >= " + qck2r1 + " and ck2 <= " + qck2r2 // TODO
                + " and ck1 = " + qck2p1
                + " and ck3 = " + qck2p2
                + " allow filtering;";
        System.out.println(":" + q2_format);

        int qck3r1 = (int)Math.floor(qck1r1abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_); // TODO double int
        int qck3r2 = (int)Math.floor(qck1r2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
        int qck3p1 = (int)Math.floor(qck1p1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_);
        int qck3p2 = (int)Math.floor(qck1p2abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
        String q3_format = "select * from " + ks + ".%s"
                + " where pkey=1 and ck3 >= " + qck3r1 + " and ck3 <= " + qck3r2 // TODO
                + " and ck1 = " + qck3p1
                + " and ck2 = " + qck3p2
                + " allow filtering;";
        System.out.println(":" + q3_format);

        int blendcases = qck1perArray.length;
        int[] chooseReal = new int[blendcases];
        int[] chooseH = new int[blendcases];
        for (int r = 0; r < blendcases; r++) { // 控制变量：改变查询占比
            //查询占比:控制变量
            int qck1per = qck1perArray[r];
            int qck2per = qck2perArray[r];
            int qck3per = qck3perArray[r];
            System.out.println("qck1/" + qck1per + " qck2/" + qck2per + " qck3/" + qck3per + " N=" + N);
            pw.write("" + qck1per + "," + qck2per + "," + qck3per + ",");

            int cfs= cflist.size();
            double[] blendRows = new double[cfs];
            double[] blendBlocks = new double[cfs];
            double[] realCost = new double[cfs];
            for (int k = 0; k < cfs; k++) { // 控制变量：compare different data models(different ACKSets)
                String cf = cflist.get(k);
                System.out.print(cf);
                System.out.print(", " + cfschemalist.get(k));

                // H公式
                H_ian h1 = new H_ian(1000000, ckn, CKdist, 1, qck1r1, qck1r2, true, true,
                        new double[]{888, qck1p1, qck1p2}, ackSeq[k]);
                H_ian h2 = new H_ian(1000000, ckn, CKdist, 2, qck2r1, qck2r2, true, true,
                        new double[]{qck2p1, 888, qck2p2}, ackSeq[k]);
                H_ian h3 = new H_ian(1000000, ckn, CKdist, 3, qck3r1, qck3r2, true, true,
                        new double[]{qck3p1, qck3p2, 888}, ackSeq[k]);
                double block1 = h1.calculate(rowSize) * qck1per;
                double block2 = h2.calculate(rowSize) * qck2per;
                double block3 = h3.calculate(rowSize) * qck3per;
                double blocks = block1 + block2 + block3;
                double row1 = h1.calculate() * qck1per;
                double row2 = h2.calculate() * qck2per;
                double row3 = h3.calculate() * qck3per;
                double rows = row1 + row2 + row3; // TODO 这里直接把rows加起来我觉得不是很妥，因为实际不是这些行累加来读的
                System.out.print(", " + rows+", "+blocks+", ");
                blendRows[k] = rows;
                blendBlocks[k] = blocks;

                // 代入cf，构造查询语句
                String q1 = String.format(q1_format, cf);
                String q2 = String.format(q2_format, cf);
                String q3 = String.format(q3_format, cf);

                // warm up  25%
                for (int i = 0; i < 20; i++) {
                    ResultSet rs1 = session.execute(q1);
                    int tmp1 = rs1.all().size(); // 起到一个遍历全部结果的作用

                    ResultSet rs2 = session.execute(q2);
                    int tmp2 = rs2.all().size(); // 起到一个遍历全部结果的作用

                    ResultSet rs3 = session.execute(q3);
                    int tmp3 = rs3.all().size(); // 起到一个遍历全部结果的作用
                }

                // 实证查询
                int repeat = 1;
                double[] instance = new double[repeat];
                double sumup = 0;
                for (int m = 0; m < repeat; m++) { // TODO 这里用时太多，先这样吧
                    long elapsed = System.nanoTime();
                    for (int i = 0; i < N; i++) { // batch
                        for (int j = 0; j < qck1per; j++) { //in a batch
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
                    instance[m] = (elapsed / (double) Math.pow(10, 6)) / N; // average batch; unit: ms
                    System.out.print("^");
                    sumup += instance[m];
                }
                sumup /= repeat;
                System.out.print(", " + sumup + ": "); // TODO mean max 80% 99%
                pw.write("" + sumup + ",");
                realCost[k] = sumup;
                for (int m = 0; m < repeat; m++) {
                    System.out.print(instance[m] + ", ");
                }
                System.out.println("");
            }
            pw.write(",");
            for(int i=0;i<cfs; i++) {
                pw.write(blendBlocks[i]+",");
            }
            pw.write(",");
            for(int i=0;i<cfs; i++) {
                pw.write(blendRows[i]+",");
            }
            pw.write(",");

            // now choose which is best for the current blend queries case
            double minH = blendBlocks[0];
            int indexH = 0;
            for(int k = 1; k < cfs; k++) {
                if(blendBlocks[k] < minH) {
                    indexH = k;
                    minH = blendBlocks[k];
                }
                else if(blendBlocks[k] ==  minH) {
                    if(blendRows[k] < blendRows[indexH]){
                        indexH = k;
                        minH = blendBlocks[k];
                    }
                }
            }
            chooseH[r] = indexH;

            double minR = realCost[0];
            int indexR=0;
            for(int k=1; k < cfs; k++) {
                if(realCost[k] < minR) {
                    minR = realCost[k];
                    indexR = k;
                }
            }
            chooseReal[r] = indexR;

            double error =(chooseH[r]-chooseReal[r])/chooseReal[r]*100;
            pw.write(chooseReal[r]+","+chooseH[r]+","+error+"\n");
            System.out.print("real->"+chooseReal[r]+" H->"+chooseH[r]);
            System.out.println("  误差(%)=(H-R)/R*10="+error+"%");

            System.out.println("---------------next line----------------");
        }
        /*
        for(int r=0;r<blendcases;r++) {
            int qck1per = qck1perArray[r];
            int qck2per = qck2perArray[r];
            int qck3per = qck3perArray[r];
            System.out.print("qck1/" + qck1per + " qck2/" + qck2per + " qck3/" + qck3per+": ");
            System.out.println("real->"+chooseReal[r]+" H->"+chooseH[r]);
        }
        */

        pw.close();
        session.close();
        cluster.close();
    }

}