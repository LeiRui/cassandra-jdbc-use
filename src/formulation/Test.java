package formulation;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        // columnspec
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

        List<Double> x2 = new ArrayList<Double>();
        for(int i = 50; i<=75; i=i+5) {
            x2.add((double)i);
        }
        List<Integer> y2 = new ArrayList<Integer>();
        y2.add(3);
        y2.add(5);
        y2.add(15);
        y2.add(5);
        y2.add(2);

        List<Double> x3 = new ArrayList<Double>();
        for(int i = 1; i<=101; i++) {
            x3.add((double)i);
        }
        List<Integer> y3 = new ArrayList<Integer>();
        for(int i = 1; i<=100; i++) {
            y3.add(1);
        }
        Column_ian ck1 = new Column_ian(step, x1, y1);
        Column_ian ck2 = new Column_ian(step, x2, y2);
        Column_ian ck3 = new Column_ian(step, x3, y3);
        List<Column_ian> CKdist = new ArrayList<Column_ian>();
        CKdist.add(ck1);
        CKdist.add(ck2);
        CKdist.add(ck3);

        int ckn = 3;

        double qck1r1 = 55;
        double qck1r2 = 60;
        double qck1p1 = 60;
        double qck1p2 = 10;

        int[][] ackSeq = new int[6][];
        ackSeq[0]=new int[]{1,2,3};
        ackSeq[1]=new int[]{1,3,2};
        ackSeq[2]=new int[]{2,1,3};
        ackSeq[3]=new int[]{3,1,2};
        ackSeq[4]=new int[]{2,3,1};
        ackSeq[5]=new int[]{3,2,1};

        for (int k = 0; k < ackSeq.length; k++) {
            // H公式
            H_ian h = new H_ian(1000000, ckn, CKdist, 2, qck1r1, qck1r2, true, false,
                    new double[]{qck1p1, 888, qck1p2}, ackSeq[k]);
            System.out.print(h.calculate()+", ");
        }
    }
}
