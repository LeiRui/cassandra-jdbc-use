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
sql已由H_SA_algorithm实例化：
 占比为：
        queriesPerc.add(1);
        queriesPerc.add(2);
        queriesPerc.add(3);
        queriesPerc.add(4);
        queriesPerc.add(4);
        queriesPerc.add(3);
        queriesPerc.add(2);
        queriesPerc.add(2);

 */
public class Test3_SAQuery {
    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";


    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        String ks = "Sirius";
        System.out.println(ks);
        List<Integer> queriesPerc = new ArrayList<Integer>();
        queriesPerc.add(1);
        queriesPerc.add(2);
        queriesPerc.add(3);
        queriesPerc.add(4);
        queriesPerc.add(4);
        queriesPerc.add(3);
        queriesPerc.add(2);
        queriesPerc.add(2);
        List<String> sqls = new ArrayList<String>();
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and " +
                "ck4=51.0 and ck7=51.0 and ck9=51.0 and ck2=51.0 and ck8=51.0 and " +
                "ck1>=1.0 and ck1<=101.0 and ck10=51.0 and ck6=51.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and " +
                "ck4=51.0 and ck7=51.0 and ck9=51.0 and ck2>=1.0 and ck2<=81.0 and " +
                "ck8=51.0 and ck1=51.0 and ck10=51.0 and ck6=51.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and " +
                "ck4>=1.0 and ck4<=31.0 and ck7=51.0 and ck9=51.0 and ck2=51.0 and " +
                "ck8=51.0 and ck1=51.0 and ck10=51.0 and ck6=51.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5>=1.0 and ck5<=91.0 and " +
                "ck3=51.0 and ck4=51.0 and ck7=51.0 and ck9=51.0 and ck2=51.0 and ck8=51.0 and " +
                "ck1=51.0 and ck10=51.0 and ck6=51.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and ck4=51.0 and " +
                "ck7=51.0 and ck9=51.0 and ck2=51.0 and ck8=51.0 and ck1=51.0 and ck10=51.0 and " +
                "ck6>=1.0 and ck6<=21.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and ck4=51.0 and " +
                "ck7>=1.0 and ck7<=51.0 and ck9=51.0 and ck2=51.0 and ck8=51.0 and ck1=51.0 and " +
                "ck10=51.0 and ck6=51.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and ck4=51.0 and " +
                "ck7=51.0 and ck9>=1.0 and ck9<=71.0 and ck2=51.0 and ck8=51.0 and ck1=51.0 and " +
                "ck10=51.0 and ck6=51.0 allow filtering;");
        sqls.add("select * from Sirius.%s where pkey=1 and ck5=51.0 and ck3=51.0 and ck4=51.0 and " +
                "ck7=51.0 and ck9=51.0 and ck2=51.0 and ck8=51.0 and ck1=51.0 and ck10>=1.0 and " +
                "ck10<=51.0 and ck6=51.0 allow filtering;");

        // table schema definition (tables are already created in cassandra)
        List<String> cflist = new ArrayList();
        List<String> cfschemalist = new ArrayList();
        int[][] ackSeq = new int[11][];
        cflist.add("dm1");
        cfschemalist.add("[3,8,6,4,9,10,2,1,7,5]");
        ackSeq[0]=new int[]{3,8,6,4,9,10,2,1,7,5};
        cflist.add("dm2");
        cfschemalist.add("[8,3,6,4,9,1,2,10,7,5]");
        ackSeq[1]=new int[]{8,3,6,4,9,1,2,10,7,5};
        cflist.add("dm3");
        cfschemalist.add("[8,3,6,4,9,10,2,1,7,5]");
        ackSeq[2]=new int[]{8,3,6,4,9,10,2,1,7,5};
        cflist.add("dm4");
        cfschemalist.add("[3,8,6,4,10,9,7,1,5,2]");
        ackSeq[3]=new int[]{3,8,6,4,10,9,7,1,5,2};
        cflist.add("dm5");
        cfschemalist.add("[8,3,2,9,10,1,5,4,6,7]");
        ackSeq[4]=new int[]{8,3,2,9,10,1,5,4,6,7};
        cflist.add("dm6");
        cfschemalist.add("[8,3,5,10,2,9,1,7,6,4]");
        ackSeq[5]=new int[]{8,3,5,10,2,9,1,7,6,4};
        cflist.add("dm7");
        cfschemalist.add("[8,6,3,9,5,1,2,10,7,4]");
        ackSeq[6]=new int[]{8,6,3,9,5,1,2,10,7,4};
        cflist.add("dm8");
        cfschemalist.add("[3,9,7,5,4,1,10,6,8,2]");
        ackSeq[7]=new int[]{3,9,7,5,4,1,10,6,8,2};
        cflist.add("dm9");
        cfschemalist.add("[6,4,3,5,7,10,2,1,9,8]");
        ackSeq[8]=new int[]{6,4,3,5,7,10,2,1,9,8};
        cflist.add("dm10");
        cfschemalist.add("[7,8,2,9,4,5,6,3,10,1]");
        ackSeq[9]=new int[]{7,8,2,9,4,5,6,3,10,1};
        cflist.add("dm11");
        cfschemalist.add("[5,3,4,7,9,2,8,1,10,6]");
        ackSeq[10]=new int[]{5,3,4,7,9,2,8,1,10,6};


        // 查询批次数
        int N = 100;

        // 控制变量：compare different data models(different ACKSets)
        for (int k = 0; k < cflist.size(); k++) {
            String cf = cflist.get(k);
            System.out.print(cf);
            System.out.print(", " + cfschemalist.get(k));

            // 实测代价
            // warm up  25%
            int sqlNum = sqls.size();
            for (int i = 0; i < 20; i++) {
                for(int j=0;j<sqlNum; j++) {
                    ResultSet rs = session.execute(String.format(sqls.get(j),cf));
                    int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                }
            }

            // 实证查询
            List<Double> resRecord = new ArrayList<Double>();
            double sumup = 0;
            for (int m = 0; m < N; m++) {
                long elapsed = System.nanoTime();
                for(int s=0;s<sqlNum;s++) { // 遍历查询集合
                    int per = queriesPerc.get(s);
                    String sql = String.format(sqls.get(s),cf);
                    for(int p=0;p<per; p++) { // 每个查询在一个批次中的执行次数（占比）
                        ResultSet rs = session.execute(sql);
                        int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
                    }
                }
                elapsed = System.nanoTime() - elapsed;
                double cost = elapsed / (double) Math.pow(10, 6); // unit: ms
                resRecord.add(cost);
                sumup += cost;
            }
            sumup /= N;
            System.out.print(String.format(", Real-Mean:%8.3f", sumup));

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
        System.out.println("---------------next line----------------");

        session.close();
        cluster.close();
    }

}
