package formulation;

import java.util.ArrayList;
import java.util.List;

public class H_ian {
    public int totalRowNumber;

    private int ckn; //排序键的数量

    private List<Column_ian> ACKdist;//按照ACK顺序映射后的排序列的分布参数

    private int qackn;//按照ACK顺序映射后的范围查询的列，从0开始

    private double qck_r1;//范围查询的左端点在该列分布的位置百分比
    private double qck_r2;//范围查询的右端点在该列分布的位置百分比
    private Column_ian.rangeType type;

    private double[] qack_p;//按照ACK顺序映射后,其余ckn-1个点查询值在该列分布的位置百分比

    public double resP; // 最后的总概率

    public H_ian(int totalRowNumber, int ckn, List<Column_ian> CKdist, int qckn, double qck_r1, double qck_r2,
                 boolean r1_closed, boolean r2_closed, double[] qck_p, int[] ackSeq){
        this.totalRowNumber = totalRowNumber;
        this.ckn = ckn;
        this.qck_r1=qck_r1;
        this.qck_r2=qck_r2;
        if(r1_closed && r2_closed)
            this.type = Column_ian.rangeType.LcRc;
        else if(r1_closed && !r2_closed)
            this.type = Column_ian.rangeType.LcRo;
        else if(!r1_closed && r2_closed)
            this.type = Column_ian.rangeType.LoRc;
        else
            this.type = Column_ian.rangeType.LoRo;
        ACKdist = new ArrayList<Column_ian>();
        qack_p = new double[ckn];
        for(int i = 0; i < ackSeq.length; i++) {//按照ACK顺序映射
            int ackindex = ackSeq[i]-1;
            ACKdist.add(CKdist.get(ackindex));
            qack_p[i]=qck_p[ackindex];
            if(ackSeq[i] == qckn) {
                qackn = i;
            }
        }
    }

    public double calculate() {
        resP = 1;
        for (int i = 0; i < qackn; i++) { // qackn是从0开始的，这里刚好表示qckn-1个
            resP *= ACKdist.get(i).getPoint(qack_p[i]);
        }
        switch (type) {
            case LcRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRc);
                break;
            case LcRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRo);
                break;
            case LoRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRc);
                break;
            case LoRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRo);
                break;
        }
        return resP*totalRowNumber;
    }


    public double calculate(int rowSize) {
        resP = 1;
        for (int i = 0; i < qackn; i++) { // qackn是从0开始的，这里刚好表示qckn-1个
            resP *= ACKdist.get(i).getPoint(qack_p[i]);
        }
        switch (type) {
            case LcRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRc);
                break;
            case LcRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LcRo);
                break;
            case LoRc:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRc);
                break;
            case LoRo:
                resP *= ACKdist.get(qackn).getBetween(qck_r1, qck_r2, Column_ian.rangeType.LoRo);
                break;
        }
        return Math.ceil(resP*totalRowNumber*rowSize/65536); // TODO
    }
}
