package mrstat;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;

public class MrStatUtil
{
	public static boolean rsrpRight(DT_Sample_4G sample, int sourceType)
	{
		int rsrp = 0;
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			rsrp = sample.LteScRSRP;
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			rsrp = sample.LteScRSRP_LT;
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			rsrp = sample.LteScRSRP_DX;
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			rsrp = sample.LteScRSRP;
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			rsrp = sample.LteScRSRP;
		}
		return rsrp >= -150 && rsrp <= -30;
	}
}
