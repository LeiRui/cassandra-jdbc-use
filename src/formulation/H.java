package formulation;

/*
 根据输入参数，计算出单查询的H建模代价，H建模代价本质上是根据各列分布，估算出seek到查询点集合的路径上的无关点的期望点数

 输入参数：
	PN: 理论总点数（不去重）
	ckn: 排序键的数量
	ckn个排序列的分布参数（为简单起见，就用自然序号唯一索引一个ck列）：is_u均匀分布/正态分布，分布参数dis_a，分布参数dis_b
	查询参数：
    	qckn：范围查询的列（自然序号） 从1开始
    	qck_r1：范围查询的左端点在该列分布的位置百分比
    	qck_r2：范围查询的右端点在该列分布的位置百分比
    	qck_p1, qck_p2, ….：其余ckn-1个点查询值在该列分布的位置百分比（顺次对应自然序号，范围列用废值填充补位）
	各列ACK排序（自然序号代表的CK列的排序，比如1-3-2）

输出结果：
从起始位置seek到范围查询目标点集合的路径上的无关点的理论概率期望数量，
E[N(A_CKSet,Q_i )]≈P(一个数据点是目标点seek路径上的无关点的概率)*总点数
以及中间seek步骤长度集合
 */

import jnr.ffi.annotations.In;

import java.util.ArrayList;
import java.util.List;

public class H {
    private int PN; //理论总点数（不去重）

    private int ckn; //排序键的数量

    private List<List> ACKdist;//按照ACK顺序映射后的排序列的分布参数

    private int qackn;//按照ACK顺序映射后的范围查询的列，从0开始

    private double qck_r1;//范围查询的左端点在该列分布的位置百分比
    private double qck_r2;//范围查询的右端点在该列分布的位置百分比

    private double[] qack_p;//按照ACK顺序映射后,其余ckn-1个点查询值在该列分布的位置百分比

    //public List<Double> steps; // seek中间步骤结果

    public double resP; // 最后的总概率
    //public double resN; // 最后的总长度

    H(int PN, int ckn, List<List> CKdist, int qckn, double qck_r1, double qck_r2, double[] qck_p, int[] ackSeq){
        this.PN = PN;
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

    public void calculate() {
        resP=0;
        double tmp=1;
        for(int i=0;i<qackn;i++) { // qackn是从0开始的，这里刚好表示qckn-1个
            resP+=tmp*getLessThan(i,qack_p[i]);
            tmp*=getPoint(i,qack_p[i]);
        }
        // 接下来判断最后一项 根据qack是不是等于ckn-1来分支
        // 如果是最后一列，+P(x_1=v_1,x_2=v_2,…,x_(i-1)=v_(i-1),x_i<v_i1 )
        // 如果不是最后一列，+P(x_1=v_1,x_2=v_2,…,x_(i-1)=v_(i-1),x_i≤v_iend )
        if(qackn == ckn-1) {
            resP+=tmp*getLessThan(qackn,qck_r1);
        }
        else {
            resP+=tmp*getLessOrEqual(qackn,qck_r2);
        }
    }

    /**
     *
     * @param index ack排序下的列序号，从0开始
     * @param pos
     * @return 该列分布下的P(x<pos)
     */
    private double getLessThan(int index, double pos) {
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
