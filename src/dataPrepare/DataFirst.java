package dataPrepare;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

public class DataFirst {
    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("panda.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        pw.write("pkey,ck1,ck2,ck3,value\n");
        int pkey = 1;
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
        pw.close();

    }
}
