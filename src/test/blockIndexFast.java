package test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import formulation.Column_ian;
import formulation.H_ian;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
   “A block index(stored at the end of the SSTable) is used to locate blocks;
   the index is loaded into memory when the SSTable is opened. A lookup can be performed with a single disk seek:
   we first find the appropriate block by performing a binary search in the in-memory index, and then reading the
   appropriate block from disk.

   H model is built based on the assumption that the fact is the cost of locating and skip to the first block to read
   is small(cassandra在如何更块地skip blocks这方面做了很多努力@Support large partitions on the 3.0 sstable format
    @promoted index @technique for skipping in very long rows
    这些都属于sstable的operations
   to help fast query)
   ) and can be dismissed compared to the cost of reading data from disk.

   1. first find the appropriate block by performing a binary search in the im-memory index
   2. then reading the appropriate block from disk
   3. filtering the candidate data into the final result set
   要说明的是H模型基于了现有的sstable的快速access机制，所以第完善、。
   一步的代价相对后面两步来说特别小，小到不会对不同ck排序模型的query evaluation
   的评价结果（最好的是哪个）产生影响，基于这一点，H建模中忽略了它。
   其实它如果真的是在内存中二分查找的话，本来也和offset关系不大，不同offset下几乎为常数，再加上内存特别快，所以这个常数又特别小，所以忽略了。
 */
public class blockIndexFast {
        public static Cluster cluster;
        public static Session session;
        private static String nodes = "127.0.0.1";

        private static int ckn = 3;

        public static void main(String[] args) {
            PrintWriter pw = null;
            PrintWriter pw_real_detail = null; // 用来记录实测数据的分位数的，pw中只有mean
            try {
                pw = new PrintWriter(new FileOutputStream("rabbit_blockIndexFast.csv"));
                pw_real_detail = new PrintWriter(new FileOutputStream("rabbit_blockIndexFast_real_detail.csv"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
            Session session = cluster.connect();

            String ks = "panda";
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
            s = "";
            for (int i = 0; i < cfschemalist.size(); i++) {
                s = s + cfschemalist.get(i) + ":mean,";
                s = s + cfschemalist.get(i) + ":min,";
                s = s + cfschemalist.get(i) + ":80th percentile,";
                s = s + cfschemalist.get(i) + ":95th percentile,";
                s = s + cfschemalist.get(i) + ":max,";
                s += ",";
            }
            pw_real_detail.write(s + "\n");


            // 查询批次数
            int N = 100;

            // 暂固定查询范围及点参
            double []qck1r1absGroup = new double[]{0,  0.1, 0.2,0.3, 0.4, 0.5,0.6, 0.7, 0.8,0.9};
            double []qck1r2absGroup = new double[]{0.1,0.2, 0.3,0.4, 0.5, 0.6,0.7, 0.8, 0.9, 1};
            double qck1p1abs = 0.5;
            double qck1p2abs = 0.5;

            // columnspec
            System.out.println("cks dist: 同分布，step=1, ck1:U[1,100], ck2:U[1,100], ck3:U[1,100]");

            for(int ii=0;ii<qck1r1absGroup.length;ii++) {
                double qck1r1abs=qck1r1absGroup[ii];
                double qck1r2abs=qck1r2absGroup[ii];

                //查询范围及点参
                System.out.println("qck1([" + qck1r1abs + "," + qck1r2abs + "]," + qck1p1abs + "," + qck1p2abs + ")");

                // qck
                int qck1r1 = (int) Math.floor(qck1r1abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_); // TODO double int
                int qck1r2 = (int) Math.floor(qck1r2abs * (ck1.xmax_ - ck1.xmin_) + ck1.xmin_);
                int qck1p1 = (int) Math.floor(qck1p1abs * (ck2.xmax_ - ck2.xmin_) + ck2.xmin_);
                int qck1p2 = (int) Math.floor(qck1p2abs * (ck3.xmax_ - ck3.xmin_) + ck3.xmin_);
                String q1_format = "select * from " + ks + ".%s"
                        + " where pkey=1 and ck1 > " + qck1r1 + " and ck1 < " + qck1r2 // TODO
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
                    //HwithoutPrefix h = new HwithoutPrefix(24,65536,1000000,3,data_dist, 1,qck1r1abs,qck1r2abs,new double[]{888,qck1p1abs,qck1p2abs},ackSeq[k]);
                    H_ian h = new H_ian(1000000, ckn, CKdist, 1, qck1r1, qck1r2, true, true,
                            new double[]{888, qck1p1, qck1p2}, ackSeq[k]);
                    System.out.print(", " + h.calculate() + ", " + h.calculate(24));

                    // 代入cf，构造查询语句
                    String q1 = String.format(q1_format, cf);

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
                    pw.write("" + sumup + ","); // 平均值
                    // 统计min,80th percentile,95th percentile,max
                    Collections.sort(resRecord);
                    int eighty_index = (int) Math.ceil(N * 0.8);
                    int ninety_five_index = (int) Math.ceil(N * 0.95);
                    System.out.println(", mean:" + sumup + ", min:" + resRecord.get(0)
                            + ", 80th percentile:" + resRecord.get(eighty_index - 1)
                            + ", 95th percentile:" + resRecord.get(ninety_five_index - 1)
                            + ", max:" + resRecord.get(resRecord.size() - 1));
                    pw_real_detail.write("" + sumup + "," + resRecord.get(0) + "," + resRecord.get(eighty_index - 1)
                            + "," + resRecord.get(ninety_five_index - 1) + "," + resRecord.get(resRecord.size() - 1)
                            + ",,");

                }
                System.out.println("---------------next line----------------");
                pw.write("\n");
                pw_real_detail.write("\n");
            }


            pw.close();
            pw_real_detail.close();
            session.close();
            cluster.close();
        }

    }
