package dataPrepare;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

public class DataFourth {
    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("gym.csv")); //Sirius
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        pw.write("pkey,ck1,ck2,ck3,ck4,ck5,ck6,ck7,ck8,ck9,ck10,value\n");
        int pkey = 1;
        Random random = new Random();
        for(int i = 0;i<2000000;i++) {
            pw.write(""+pkey);
            for(int j = 0; j < 10; j++) {
                pw.write(","+(random.nextInt(100)+1));
            }
            pw.write(","+random.nextInt(1000)+"\n");
        }
        /*
        Random random = new Random();
        for (int ck1 = 1; ck1 <= 100; ck1++) {
            for (int ck2 = 1; ck2 <= 100; ck2++) {
                for (int ck3 = 1; ck3 <= 100; ck3++) {
                    pw.write("" + pkey + "," + ck1 + "," + ck2 + "," + ck3 + ","
                            + random.nextInt(100)
                            + "\n");
                }
            }
        }
        */

        pw.close();

    }
}
