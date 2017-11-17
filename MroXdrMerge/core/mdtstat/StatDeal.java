package mdtstat;

import StructData.DT_Sample_23G;
import StructData.DT_Sample_4G;
import mrstat.TypeResult;

public class StatDeal
{
	protected DayDataDeal_4G_YD dayDataDeal_4G_YD;
	protected DayDataDeal_4G_LT dayDataDeal_4G_LT;
	protected DayDataDeal_4G_DX dayDataDeal_4G_DX;

	public StatDeal(TypeResult typeResult)
	{
		// 移动
		dayDataDeal_4G_YD = new DayDataDeal_4G_YD(typeResult);
		// 联通
		dayDataDeal_4G_LT = new DayDataDeal_4G_LT(typeResult);
		// 电信
		dayDataDeal_4G_DX = new DayDataDeal_4G_DX(typeResult);
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
		// 统计移动指标
		dayDataDeal_4G_YD.dealSample(sample);
		// 统计联通指标
		dayDataDeal_4G_LT.dealSample(sample);
		// 统计电信指标
		dayDataDeal_4G_DX.dealSample(sample);
	}

	public void dealSample(DT_Sample_23G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
	}

	public int outResult()
	{
		dayDataDeal_4G_YD.outResult();
		dayDataDeal_4G_LT.outResult();
		dayDataDeal_4G_DX.outResult();

		return 0;
	}

}
