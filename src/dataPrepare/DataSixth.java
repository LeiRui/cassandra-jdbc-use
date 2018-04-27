package dataPrepare;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

/*
为了Test4 blockReadLadder 生成最后一列特别多的数据
 */
public class DataSixth {
    public static void main(String[] args) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("red.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        pw.write("pkey,ck1,ck2,ck3,value\n");
        int pkey = 1;
        int ck1 = 1;
        int ck2 = 1;
        Random random = new Random();
        for (int ck3 = 1; ck3 <= 1000000; ck3++) {
            pw.write("" + pkey + "," + ck1 + "," + ck2 + "," + ck3 + ","
                    + random.nextInt(100)
                    + "\n");
        }
        pw.close();

    }
}
