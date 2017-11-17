package mdtstat;

import StructData.DT_Sample_4G;
import mrstat.TypeResult;

public class UserMdtStat
{
	private StatDeal statDeal;

	public UserMdtStat(TypeResult typeResult)
	{
		statDeal = new StatDeal(typeResult);
	}

	public int dealSample(DT_Sample_4G sample)
	{
		statDeal.dealSample(sample);
		return 0;
	}

	public int outResult()
	{
		statDeal.outResult();
		return 0;
	}

}
