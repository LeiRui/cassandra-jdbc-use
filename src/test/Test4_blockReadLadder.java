package test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
  印证cassandra的block读取代价的阶梯性
  表结构不变 改变查询范围
 */
public class Test4_blockReadLadder {
    public static Cluster cluster;
    public static Session session;
    private static String nodes = "127.0.0.1";


    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("test4.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();

        //String sql_format = "select * from red.dm1 where pkey=1 and ck1=1 and ck2=1 and ck3>=1 and ck3<=%d allow filtering;";
        String sql_format = "select * from panda.dm1 where pkey=1 and ck1=1 and ck2>=1 and ck2<=%d and ck3=1 allow filtering;";


        // 查询批次数
        int N = 100;

        //for (int z = 10000; z < 1000000; z=z+10000) {
        for (int z = 1; z < 100; z++) {
            //pw.write(z+",");
            pw.write(z*100+",");
            //pw.write(z*24/65536+",");
            pw.write((double)z*100*24/65536+",");
            //pw.write(Math.ceil(z*24/65536)+",");
            pw.write(Math.ceil((double)z*100*24/65536)+",");
            String sql = String.format(sql_format, z);
            System.out.println(sql);

            // 实测代价
            // warm up  25%
            for (int i = 0; i < 20; i++) {
                ResultSet rs = session.execute(sql);
                int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
            }

            // 实证查询
            List<Double> resRecord = new ArrayList<Double>();
            double sumup = 0;
            for (int m = 0; m < N; m++) {
                long elapsed = System.nanoTime();
                ResultSet rs = session.execute(sql);
                int tmp = rs.all().size(); // 起到一个遍历全部结果的作用
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
                    , resRecord.get(0)
                    , resRecord.get(eighty_index - 1)
                    , resRecord.get(ninety_five_index - 1)
                    , resRecord.get(resRecord.size() - 1)));
            pw.write(sumup+"\n");
        }
        pw.close();
        session.close();
        cluster.close();
    }

}

