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


public class Test2_blendQuery{

    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";

    private static int ckn = 3;

    private static int rowSize =24;

    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("test2.csv"));
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

        // 范围查询参数
        double qck1r1abs = 0;
        double qck1r2abs = 0.05;
        double qck1p1abs = 0.2;
        double qck1p2abs = 0.3;

        double qck2r1abs = 0.4;
        double qck2r2abs = 0.5;
        double qck2p1abs = 0.5;
        double qck2p2abs = 0.5;

        double qck3r1abs = 0.5;
        double qck3r2abs = 0.7;
        double qck3p1abs = 0.6;
        double qck3p2abs = 0.3;

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

        System.out.print("qck2("+qck2p1abs+",["+qck2r1abs+","+qck2r2abs+"],"+qck2p2abs+")");
        int qck2r1 = (int)Math.floor(qck2r1abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_); // TODO double int
        int qck2r2 = (int)Math.floor(qck2r2abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
        int qck2p1 = (int)Math.floor(qck2p1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_);
        int qck2p2 = (int)Math.floor(qck2p2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
        String q2_format = "select * from " + ks + ".%s"
                + " where pkey=1 and ck2 >= " + qck2r1 + " and ck2 <= " + qck2r2 // TODO
                + " and ck1 = " + qck2p1
                + " and ck3 = " + qck2p2
                + " allow filtering;";
        System.out.println(":" + q2_format);

        System.out.print("qck3("+qck3p1abs+","+qck3p2abs+",["+qck3r1abs+","+qck3r2abs+"])");
        int qck3r1 = (int)Math.floor(qck3r1abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_); // TODO double int
        int qck3r2 = (int)Math.floor(qck3r2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
        int qck3p1 = (int)Math.floor(qck3p1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_);
        int qck3p2 = (int)Math.floor(qck3p2abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
        String q3_format = "select * from " + ks + ".%s"
                + " where pkey=1 and ck3 >= " + qck3r1 + " and ck3 <= " + qck3r2 // TODO
                + " and ck1 = " + qck3p1
                + " and ck2 = " + qck3p2
                + " allow filtering;";
        System.out.println(":" + q3_format);


        // 查询占比
        int[] qck1perArray = new int[]{8, 8, 7, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1 };
        int[] qck2perArray = new int[]{2, 1, 2, 1, 4, 3, 1, 0, 6, 5, 4, 1, 0, 7, 6, 4, 2, 1, 7, 3, 2, 1, 0, 8, 6, 5, 3, 1 };
        int[] qck3perArray = new int[]{0, 1, 1, 3, 1, 2, 4, 5, 0, 1, 2, 5, 6, 0, 1, 3, 5, 6, 1, 5, 6, 7, 8, 1, 3, 4, 6, 8 };


        int N=10; // 查询批次
        // 770*5ms*10*6*60

        int blendcases = qck1perArray.length;
        int[] chooseReal = new int[blendcases];
        int[] chooseH = new int[blendcases];
        for (int r = 0; r < blendcases; r++) { // 控制变量1：改变查询占比
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
            for (int k = 0; k < cfs; k++) { // 控制变量2：compare different data models(different ACKSets)
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
                int block1 = h1.calculate(rowSize) * qck1per;
                int block2 = h2.calculate(rowSize) * qck2per;
                int block3 = h3.calculate(rowSize) * qck3per;
                int blocks = block1 + block2 + block3;
                double row1 = h1.calculate() * qck1per;
                double row2 = h2.calculate() * qck2per;
                double row3 = h3.calculate() * qck3per;
                double rows = row1 + row2 + row3; // TODO 这里直接把rows加起来我觉得不是很妥，因为实际不是这些行累加来读的
                //System.out.print(", " + rows+", "+blocks+", ");
                System.out.print(String.format(", HR:%8d, HB:%4d", Math.round(rows),blocks));
                blendRows[k] = rows;
                blendBlocks[k] = blocks;

                // 代入cf，构造查询语句
                String q1 = String.format(q1_format, cf);
                String q2 = String.format(q2_format, cf);
                String q3 = String.format(q3_format, cf);


                // warm up  25%
                for (int i = 0; i < 10; i++) {
                    ResultSet rs1 = session.execute(q1);
                    int tmp1 = rs1.all().size(); // 起到一个遍历全部结果的作用

                    ResultSet rs2 = session.execute(q2);
                    int tmp2 = rs2.all().size(); // 起到一个遍历全部结果的作用

                    ResultSet rs3 = session.execute(q3);
                    int tmp3 = rs3.all().size(); // 起到一个遍历全部结果的作用
                }

                // 实证查询
                List<Double> resRecord = new ArrayList<Double>();
                double sumup = 0;
                for (int m = 0; m < N; m++) {
                    long elapsed = System.nanoTime();
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
                    elapsed = System.nanoTime() - elapsed;
                    double cost = elapsed / (double) Math.pow(10, 6); // unit: ms
                    resRecord.add(cost);
                    sumup += cost;
                }
                sumup /= N;
                System.out.print(String.format(", Real-Mean:%8.3f", sumup));
                pw.write("" + sumup + ","); // 平均值
                realCost[k] = sumup;

                // 统计min,80th percentile,95th percentile,max
                Collections.sort(resRecord);
                int eighty_index = (int) Math.ceil(N * 0.8);
                int ninety_five_index = (int) Math.ceil(N * 0.95);
                System.out.println(String.format(", min:%8.3f, 80th percentile:%8.3f, 95th percentile:%8.3f, max:%8.3f"
                        ,resRecord.get(0)
                        ,resRecord.get(eighty_index - 1)
                        ,resRecord.get(ninety_five_index - 1)
                        ,resRecord.get(resRecord.size() - 1)));
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

            double error =(realCost[chooseH[r]]-realCost[chooseReal[r]])/realCost[chooseReal[r]]*100;
            pw.write(chooseReal[r]+","+chooseH[r]+","+error+"\n");
            System.out.print("real->"+(chooseReal[r]+1)+" H->"+(chooseH[r]+1)); // +1是因为从0开始的问题
            System.out.println(String.format("  误差百分比=(RCH-RCR)/RCR*100=%.4f",error)+"%");

            System.out.println("---------------next line----------------");
        }

        pw.close();
        session.close();
        cluster.close();
    }

}