package formulation;

/*
 根据输入参数，计算出单查询的H建模代价，H建模代价本质上是根据各列分布，估算出从第一个查询点到最后一个查询点覆盖的总点数（有效的查询读取点数包括在内）

 输入参数：
	totalRowNumber: 研究的一个partition的数据总行数
	ckn: 排序键的数量
	ckn个排序列的分布参数（为简单起见，就用自然序号唯一索引一个ck列）：is_u均匀分布/正态分布，分布参数dis_a，分布参数dis_b
	查询参数：
    	qckn：范围查询的列（自然序号） 从1开始
    	qck_r1：范围查询的左端点在该列分布的位置百分比
    	qck_r2：范围查询的右端点在该列分布的位置百分比
    	qck_p1, qck_p2, ….：其余ckn-1个点查询值在该列分布的位置百分比（顺次对应自然序号，范围列用废值填充补位）
	各列ACK排序（自然序号代表的CK列的排序，比如1-3-2）

 */

import jnr.ffi.annotations.In;

import java.util.ArrayList;
import java.util.List;

public class HwithoutPrefix {
    private int rowSize; // bytes
    private int blockSize; // bytes
    private int totalRowNumber;

    private int ckn; //排序键的数量

    private List<List> ACKdist;//按照ACK顺序映射后的排序列的分布参数

    private int qackn;//按照ACK顺序映射后的范围查询的列，从0开始

    private double qck_r1;//范围查询的左端点在该列分布的位置百分比
    private double qck_r2;//范围查询的右端点在该列分布的位置百分比

    private double[] qack_p;//按照ACK顺序映射后,其余ckn-1个点查询值在该列分布的位置百分比


    public double resP;
    public double cost;

    public HwithoutPrefix(int rowSize, int blockSize, int totalRowNumber, int ckn, List<List> CKdist, int qckn, double qck_r1, double qck_r2, double[] qck_p, int[] ackSeq){
        this.rowSize = rowSize;
        this.blockSize = blockSize;
        this.totalRowNumber = totalRowNumber;
        this.ckn = ckn;
        this.qck_r1=qck_r1;
        this.qck_r2=qck_r2;
        ACKdist = new ArrayList<List>();
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

    public double Cost(double averageBlockCost) {
        int blocknum = (int)Math.ceil((calculateWithoutPrefix() * totalRowNumber)* rowSize / blockSize);
        if(blocknum == 0)
            blocknum = 1;
        cost = averageBlockCost *blocknum;
        return cost;
    }

    public double calculateWithoutPrefix() {
        resP=0;

        double fix = 1;
        for(int i=0;i<qackn;i++) { // qackn是从0开始的，这里刚好表示qckn-1个
            fix *= getPoint(i, qack_p[i]);
        }

        double a = getLessOrEqual(qackn,qck_r2);

        double b = 0;
        double tmp = 1;
        qack_p[qackn] = qck_r1;
        for (int i = qackn; i < ckn; i++) {
            b += tmp * getLessThan(i, qack_p[i]);
            tmp *= getPoint(i, qack_p[i]);
        }

        resP = fix*(a-b);
        return resP;
    }

    /**
     *
     * @param index ack排序下的列序号，从0开始
     * @param pos
     * @return 该列分布下的P(x<pos)
     */
    private double getLessThan(int index, double pos) {
        // TODO VIP!!! 待修复的bug：即便是均匀分布，也不是说查询范围20%概率就是20%！
        // TODO 比如均匀分布就三个点0、50、100，然后查询范围20%-40%指的是20-40，不代表20-40内就真的有20%的数据分布了！！！
        // TODO 说到底，这是离散分布带来的问题 必须解决


        // TODO 正态分布待补充
        if((Boolean)(ACKdist.get(index).get(0))) {
            return pos; // TODO 这是均匀分布的，并且连续和离散的细节暂时不处理
        }
        else {
            return pos;// TODO 这是均匀分布的，并且连续和离散的细节暂时不处理
        }
    }

    /**
     *
     * @param index ack排序下的列序号，从0开始
     * @param pos
     * @return 该列分布下的P(x<=pos)
     */
    private double getLessOrEqual(int index, double pos) {
        // TODO 正态分布待补充
        return pos;// TODO 这是均匀分布的，并且连续和离散的细节暂时不处理
    }

    /**
     *
     * @param index ack排序下的列序号，从0开始
     * @param pos
     * @return 该列分布下的P(x=pos)
     */
    private double getPoint(int index, double pos) {
        // TODO 正态分布待补充
        double dis_a = Double.valueOf((ACKdist.get(index).get(1)).toString());
        double dis_b = Double.valueOf((ACKdist.get(index).get(2)).toString());
        double res = 1/(dis_b-dis_a+1); // TODO 这里是离散均匀分布
        return res;
    }



}
