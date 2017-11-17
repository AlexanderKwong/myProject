package mrstat;

import StructData.DT_Sample_4G;

public class UserMrStat
{
	// private StatDeal statDeal;

	private StatDeals statDeals;

	public UserMrStat(TypeResult typeResult)
	{
		statDeals = new StatDeals(typeResult);
	}

	public int dealSample(DT_Sample_4G sample)
	{
		statDeals.dealSample(sample);
		return 0;
	}

	public int outResult()
	{
		statDeals.outResult();
		return 0;
	}

}
