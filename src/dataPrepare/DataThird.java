package dataPrepare;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*
 按照直方图倒过来生成模拟数据
 */
public class DataThird {
    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("normdata2.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        pw.write("pkey,ck1,ck2,ck3,value1,value2,value3\n");

        int pkey = 1; // important

        // 数据分布参数
        double step = 1;
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

        int sum_=0;
        for(int num: y1) {
            sum_+=num;
        }

        // 细化离散点的概率
        double xmin = x1.get(0);
        double xmax = x1.get(x1.size()-1);
        int num= (int)((xmax - xmin)/step);
        double[] x_ = new double[num];
        double[][] a_ = new double[num][2];

        int index = 0;
        double previous = 0;
        for(int i = 0; i < x1.size()-1; i++) {
            int number = (int)((x1.get(i+1)-x1.get(i))/step);
            double pos = x1.get(i);
            while(pos < x1.get(i+1)) {
                x_[index] = pos;
                a_[index] = new double[2];
                a_[index][0] = previous;
                previous += (double)(y1.get(i))/(sum_*number);
                a_[index][1] = previous;
                index++;
                pos += step;
            }
        }

        int totalRowNumber = 1000000;

        Random r = new Random();
        for(int i = 0; i < totalRowNumber; i++) {
            double random = Math.random();
            int j = 0;
            for(; j < num; j++) {
                if(random >=a_[j][0] && random < a_[j][1]) {
                    break;
                }
            }
            pw.write("" + pkey + "," + x_[j] +","+ r.nextInt(1000)+","+r.nextInt(10000)
                    + "," + r.nextInt(100)
                    + "," + r.nextInt(100)
                    + "," + r.nextInt(100)
                    + "\n");
        }
        pw.close();

    }
}
