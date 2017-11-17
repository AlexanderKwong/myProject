package mrstat.struct;

import StructData.LteScPlrQciData;

/**
 * Qci丢包率统计，及其输出
 * @author Kwong
 */
public class StatLteQci
{
	public int[] iArrLteScPlrULQciCnt;
	public double[] iArrLteScPlrULQciValue;
	public int[] iArrLteScPlrDLQciCnt;
	public double[] iArrLteScPlrDLQciValue;
	
	public StatLteQci(){
		iArrLteScPlrULQciCnt = new int[LteScPlrQciData.MR_QCI_ARRAY_LENGTH];
		iArrLteScPlrDLQciCnt = new int[LteScPlrQciData.MR_QCI_ARRAY_LENGTH];
		iArrLteScPlrULQciValue = new double[LteScPlrQciData.MR_QCI_ARRAY_LENGTH];
		iArrLteScPlrDLQciValue = new double[LteScPlrQciData.MR_QCI_ARRAY_LENGTH];
	}
	
	/*	public static void main(String[] args){
	   double sum = 0;
	   for(int i = 0; i< Integer.MAX_VALUE; i++){
		   sum += 0.5d;//0.1d会丢失
		   
		   if(sum >10000) sum -= 10000;//令其不能用科学记数法表示
		   String v = String.valueOf(sum);
		   if(v.substring(v.lastIndexOf("."), v.length()).length()>2){
		   		System.out.println(sum);
		   		throw new RuntimeException("精度丢失");
		   }
	   }
	}*/
	
	/*
	 * statPacketLossQCI
	 */
	public void statPacketLossQCI(LteScPlrQciData qciData)
	{
		//TODO 经测试 .5 相加不会产生精度丢失，出于性能考虑这里直接用基本类型；否则必须使用BigDecimal进行运算
		double tmp = -1D;
		for(int i = 0; i < LteScPlrQciData.MR_QCI_ARRAY_LENGTH; i++){
			if((tmp = qciData.formatedDLQci[i]) > 0){
				iArrLteScPlrDLQciCnt[i]++;
				iArrLteScPlrDLQciValue[i] += tmp;
			}
			if((tmp = qciData.formatedULQci[i]) > 0){
				iArrLteScPlrULQciCnt[i]++;
				iArrLteScPlrULQciValue[i] += tmp;
			}
		}
	}
	
	public void toString(StringBuffer bf, String spliter){
		for(int[] arr : new int[][]{iArrLteScPlrULQciCnt, iArrLteScPlrDLQciCnt})
			for(int i : arr)
				bf.append(i).append(spliter);
		for(double[] arr : new double[][]{iArrLteScPlrULQciValue, iArrLteScPlrDLQciValue})
			for(double i : arr)
				bf.append(i).append(spliter);

		bf.delete(bf.lastIndexOf(spliter), bf.length());
	}
}
