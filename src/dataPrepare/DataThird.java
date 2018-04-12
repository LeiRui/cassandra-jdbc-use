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
            pw = new PrintWriter(new FileOutputStream("testnorm2.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        pw.write("pkey,ck1,ck2,ck3,value1\n");

        int pkey = 1; // important

        // 数据分布参数
        // ck1
        double step1 = 1;
        List<Double> x1 = new ArrayList<Double>();
        for(int i = 50; i<=100; i=i+5) {
            x1.add((double)i);
        }
        List<Integer> y1 = new ArrayList<Integer>();
        y1.add(3);
        y1.add(4);
        y1.add(5);
        y1.add(6);
        y1.add(15);
        y1.add(15);
        y1.add(7);
        y1.add(6);
        y1.add(4);
        y1.add(1);

        int sum1_=0;
        for(int num: y1) {
            sum1_+=num;
        }

        // 细化离散点的概率
        double xmin1 = x1.get(0);
        double xmax1 = x1.get(x1.size()-1);
        int num1= (int)((xmax1 - xmin1)/step1);
        double[] x1_ = new double[num1];
        double[][] a1_ = new double[num1][2];

        int index1 = 0;
        double previous1 = 0;
        for(int i = 0; i < x1.size()-1; i++) {
            int number = (int)((x1.get(i+1)-x1.get(i))/step1);
            double pos = x1.get(i);
            while(pos < x1.get(i+1)) {
                x1_[index1] = pos;
                a1_[index1] = new double[2];
                a1_[index1][0] = previous1;
                previous1 += (double)(y1.get(i))/(sum1_*number);
                a1_[index1][1] = previous1;
                index1++;
                pos += step1;
            }
        }

        // ck2
        double step2 = 1;
        List<Double> x2 = new ArrayList<Double>();
        for(int i = 0; i<=180; i=i+20) {
            x2.add((double)i);
        }
        List<Integer> y2 = new ArrayList<Integer>();
        y2.add(16);
        y2.add(212);
        y2.add(1416);
        y2.add(3354);
        y2.add(3433);
        y2.add(1335);
        y2.add(220);
        y2.add(17);
        y2.add(2);

        int sum2_=0;
        for(int num: y2) {
            sum2_+=num;
        }

        // 细化离散点的概率
        double xmin2 = x2.get(0);
        double xmax2 = x2.get(x2.size()-1);
        int num2= (int)((xmax2 - xmin2)/step2);
        double[] x2_ = new double[num2];
        double[][] a2_ = new double[num2][2];

        int index2 = 0;
        double previous2 = 0;
        for(int i = 0; i < x2.size()-1; i++) {
            int number = (int)((x2.get(i+1)-x2.get(i))/step2);
            double pos = x2.get(i);
            while(pos < x2.get(i+1)) {
                x2_[index2] = pos;
                a2_[index2] = new double[2];
                a2_[index2][0] = previous2;
                previous2 += (double)(y2.get(i))/(sum2_*number);
                a2_[index2][1] = previous2;
                index2++;
                pos += step2;
            }
        }

        // ck3
        double step3 = 1;
        List<Double> x3 = new ArrayList<Double>();
        for(int i = -18; i<=62; i=i+10) {
            x3.add((double)i);
        }
        List<Integer> y3 = new ArrayList<Integer>();
        y3.add(28);
        y3.add(327);
        y3.add(1746);
        y3.add(6729);
        y3.add(5877);
        y3.add(2400);
        y3.add(200);
        y3.add(30);

        int sum3_=0;
        for(int num: y3) {
            sum3_+=num;
        }

        // 细化离散点的概率
        double xmin3 = x3.get(0);
        double xmax3 = x3.get(x3.size()-1);
        int num3= (int)((xmax3 - xmin3)/step3);
        double[] x3_ = new double[num3];
        double[][] a3_ = new double[num3][2];

        int index3 = 0;
        double previous3 = 0;
        for(int i = 0; i < x3.size()-1; i++) {
            int number = (int)((x3.get(i+1)-x3.get(i))/step3);
            double pos = x3.get(i);
            while(pos < x3.get(i+1)) {
                x3_[index3] = pos;
                a3_[index3] = new double[2];
                a3_[index3][0] = previous3;
                previous3 += (double)(y3.get(i))/(sum3_*number);
                a3_[index3][1] = previous3;
                index3++;
                pos += step3;
            }
        }



        int totalRowNumber = 1000000;

        for(int i = 0; i < totalRowNumber; i++) {
            double random1 = Math.random();
            //System.out.println("random1="+random1);
            int j1 = 0;
            for(; j1 < num1; j1++) {
                if(random1 >=a1_[j1][0] && random1 < a1_[j1][1]) {
                    break;
                }
            }
            double random2 = Math.random();
            //System.out.println("random2="+random2);
            int j2 = 0;
            for(; j2 < num2; j2++) {
                if(random2 >=a2_[j2][0] && random2 < a2_[j2][1]) {
                    break;
                }
            }
            double random3 = Math.random();
            //System.out.println("random3="+random3);
            int j3 = 0;
            for(; j3 < num3; j3++) {
                if(random3 >=a3_[j3][0] && random3 < a3_[j3][1]) {
                    break;
                }
            }
            Random r = new Random();
            pw.write("" + pkey + "," + x1_[j1] +","+ x2_[j2]+","+x3_[j3]
                    + "," + r.nextInt(100)
                    + "\n");
        }
        pw.close();

    }
}
