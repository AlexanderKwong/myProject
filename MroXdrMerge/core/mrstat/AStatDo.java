package mrstat;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mro.lablefill_mro.MroLableFileReducer.MroDataFileReducers;

public abstract class AStatDo implements IStatDo
{
	protected TypeResult typeResult;
	protected long oneCount = 0;
	protected long oneCountMax = 100000;

	public AStatDo(TypeResult typeResult)
	{
		this.typeResult = typeResult;
	}

	@Override
	public int stat(Object tsam)
	{
		oneCount++;
		return statSub(tsam);
	}

	public abstract int statSub(Object tsam);

	@Override
	public int outDealingResult()
	{
		if (oneCountMax < oneCount)
		{
			oneCount = 0;
			return outDealingResultSub();
		}
		return 0;
	}

	public abstract int outDealingResultSub();

	@Override
	public int outFinalReuslt()
	{
		oneCount = 0;
		return outFinalReusltSub();
	}

	public int getIfreq(int sourceType, DT_Sample_4G sample)
	{
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
		{
			return 0;
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			return sample.LteScEarfcn_DX;
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			return sample.LteScEarfcn_LT;
		}
		return -1;
	}

	public abstract int outFinalReusltSub();

}
