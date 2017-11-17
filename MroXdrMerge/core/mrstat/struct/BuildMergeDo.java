package mrstat.struct;

import StructData.StaticConfig;
import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class BuildMergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_Build build = new Stat_Build();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(build.iCityID);
		sbTemp.append("_");
		sbTemp.append(build.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(build.iHeight);
		sbTemp.append("_");
		sbTemp.append(build.ifreq);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(build.iTime));
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		// TODO Auto-generated method stub
		BuildMergeDo temp = (BuildMergeDo) o;
		build.iMRCnt += temp.build.iMRCnt;
		// build.iMRCnt_In_URI += temp.build.iMRCnt_In_URI;
		// build.iMRCnt_In_SDK += temp.build.iMRCnt_In_SDK;
		// build.iMRCnt_In_WLAN += temp.build.iMRCnt_In_WLAN;
		// build.iMRCnt_In_SIMU += temp.build.iMRCnt_In_SIMU;
		// build.iMRCnt_In_Other += temp.build.iMRCnt_In_Other;
		build.iMRSINRCnt += temp.build.iMRSINRCnt;
		build.fRSRPValue += temp.build.fRSRPValue;
		build.fRSRQValue += temp.build.fRSRQValue;
		build.fSINRValue += temp.build.fSINRValue;
		build.iMRCnt_95 += temp.build.iMRCnt_95;
		build.iMRCnt_100 += temp.build.iMRCnt_100;
		build.iMRCnt_103 += temp.build.iMRCnt_103;
		build.iMRCnt_105 += temp.build.iMRCnt_105;
		build.iMRCnt_110 += temp.build.iMRCnt_110;
		build.iMRCnt_113 += temp.build.iMRCnt_113;
		build.iMRCnt_128 += temp.build.iMRCnt_128;
		build.iRSRP100_SINR0 += temp.build.iRSRP100_SINR0;
		build.iRSRP105_SINR0 += temp.build.iRSRP105_SINR0;
		build.iRSRP110_SINR3 += temp.build.iRSRP110_SINR3;
		build.iRSRP110_SINR0 += temp.build.iRSRP110_SINR0;
		build.iSINR_0 += temp.build.iSINR_0;
		build.iRSRQ_14 += temp.build.iRSRQ_14;
		build.fOverlapTotal += temp.build.fOverlapTotal;
		build.iOverlapMRCnt += temp.build.iOverlapMRCnt;
		build.fOverlapTotalAll += temp.build.fOverlapTotalAll;
		build.iOverlapMRCntAll += temp.build.iOverlapMRCntAll;
		if (temp.build.fRSRPMax > build.fRSRPMax)
		{
			build.fRSRPMax = temp.build.fRSRPMax;
		}
		if ((build.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.build.fRSRPMin < build.fRSRPMin && temp.build.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			build.fRSRPMin = temp.build.fRSRPMin;
		}
		if (temp.build.fRSRQMax > build.fRSRQMax)
		{
			build.fRSRQMax = temp.build.fRSRQMax;
		}
		if ((build.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.build.fRSRQMin < build.fRSRQMin && temp.build.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			build.fRSRQMin = temp.build.fRSRQMin;
		}
		if (temp.build.fSINRMax > build.fSINRMax)
		{
			build.fSINRMax = temp.build.fSINRMax;
		}
		if ((build.fSINRMin == StaticConfig.Int_Abnormal) || (temp.build.fSINRMin < build.fSINRMin && temp.build.fSINRMin != StaticConfig.Int_Abnormal))
		{
			build.fSINRMin = temp.build.fSINRMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		try
		{
			build = Stat_Build.FillData(vals, 0);
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return build.roundDayToLine();
	}

}
