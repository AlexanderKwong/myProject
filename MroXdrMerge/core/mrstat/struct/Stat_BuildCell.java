package mrstat.struct;

import StructData.DT_Sample_4G;
import jan.util.TimeHelper;
import mdtstat.Util;

public class Stat_BuildCell extends Stat_Build
{
	public int iEci;

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ispeed;
		iHeight = sample.imode;
		iTime = Util.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
		iEci = (int) sample.Eci;
	}

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
		bf.append(spliter);
		bf.append(iEci);
		bf.append(spliter);
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(iMRSINRCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(fSINRValue);
		bf.append(spliter);
		bf.append(iMRCnt_95);
		bf.append(spliter);
		bf.append(iMRCnt_100);
		bf.append(spliter);
		bf.append(iMRCnt_103);
		bf.append(spliter);
		bf.append(iMRCnt_105);
		bf.append(spliter);
		bf.append(iMRCnt_110);
		bf.append(spliter);
		bf.append(iMRCnt_113);
		bf.append(spliter);
		bf.append(iMRCnt_128);
		bf.append(spliter);
		bf.append(iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iSINR_0);
		bf.append(spliter);
		bf.append(iRSRQ_14);
		bf.append(spliter);
		bf.append(fOverlapTotal);
		bf.append(spliter);
		bf.append(iOverlapMRCnt);
		bf.append(spliter);
		bf.append(fOverlapTotalAll);
		bf.append(spliter);
		bf.append(iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(fRSRPMax);
		bf.append(spliter);
		bf.append(fRSRPMin);
		bf.append(spliter);
		bf.append(fRSRQMax);
		bf.append(spliter);
		bf.append(fRSRQMin);
		bf.append(spliter);
		bf.append(fSINRMax);
		bf.append(spliter);
		bf.append(fSINRMin);
		return bf.toString();
	}

	public String roundDayToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
		bf.append(spliter);
		bf.append(iEci);
		bf.append(spliter);
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(TimeHelper.getRoundDayTime(iTime));
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(iMRSINRCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(fSINRValue);
		bf.append(spliter);
		bf.append(iMRCnt_95);
		bf.append(spliter);
		bf.append(iMRCnt_100);
		bf.append(spliter);
		bf.append(iMRCnt_103);
		bf.append(spliter);
		bf.append(iMRCnt_105);
		bf.append(spliter);
		bf.append(iMRCnt_110);
		bf.append(spliter);
		bf.append(iMRCnt_113);
		bf.append(spliter);
		bf.append(iMRCnt_128);
		bf.append(spliter);
		bf.append(iRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iSINR_0);
		bf.append(spliter);
		bf.append(iRSRQ_14);
		bf.append(spliter);
		bf.append(fOverlapTotal);
		bf.append(spliter);
		bf.append(iOverlapMRCnt);
		bf.append(spliter);
		bf.append(fOverlapTotalAll);
		bf.append(spliter);
		bf.append(iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(fRSRPMax);
		bf.append(spliter);
		bf.append(fRSRPMin);
		bf.append(spliter);
		bf.append(fRSRQMax);
		bf.append(spliter);
		bf.append(fRSRQMin);
		bf.append(spliter);
		bf.append(fSINRMax);
		bf.append(spliter);
		bf.append(fSINRMin);
		return bf.toString();
	}

	public static Stat_BuildCell FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_BuildCell build = new Stat_BuildCell();
		build.iCityID = Integer.parseInt(vals[i++]);
		build.iBuildingID = Integer.parseInt(vals[i++]);
		build.iHeight = Integer.parseInt(vals[i++]);
		build.iEci = Integer.parseInt(vals[i++]);
		build.ifreq = Integer.parseInt(vals[i++]);
		build.iTime = Integer.parseInt(vals[i++]);
		build.iMRCnt = Integer.parseInt(vals[i++]);
		// build.iMRCnt_In_URI = Integer.parseInt(vals[i++]);
		// build.iMRCnt_In_SDK = Integer.parseInt(vals[i++]);
		// build.iMRCnt_In_WLAN = Integer.parseInt(vals[i++]);
		// build.iMRCnt_In_SIMU = Integer.parseInt(vals[i++]);
		// build.iMRCnt_In_Other = Integer.parseInt(vals[i++]);
		build.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		build.iMRSINRCnt = Integer.parseInt(vals[i++]);
		build.fRSRPValue = Float.parseFloat(vals[i++]);
		build.fRSRQValue = Float.parseFloat(vals[i++]);
		build.fSINRValue = Float.parseFloat(vals[i++]);
		build.iMRCnt_95 = Integer.parseInt(vals[i++]);
		build.iMRCnt_100 = Integer.parseInt(vals[i++]);
		build.iMRCnt_103 = Integer.parseInt(vals[i++]);
		build.iMRCnt_105 = Integer.parseInt(vals[i++]);
		build.iMRCnt_110 = Integer.parseInt(vals[i++]);
		build.iMRCnt_113 = Integer.parseInt(vals[i++]);
		build.iMRCnt_128 = Integer.parseInt(vals[i++]);
		build.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
		build.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
		build.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
		build.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
		build.iSINR_0 = Integer.parseInt(vals[i++]);
		build.iRSRQ_14 = Integer.parseInt(vals[i++]);
		build.fOverlapTotal = Float.parseFloat(vals[i++]);
		build.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		build.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		build.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		build.fRSRPMax = Float.parseFloat(vals[i++]);
		build.fRSRPMin = Float.parseFloat(vals[i++]);
		build.fRSRQMax = Float.parseFloat(vals[i++]);
		build.fRSRQMin = Float.parseFloat(vals[i++]);
		build.fSINRMax = Float.parseFloat(vals[i++]);
		build.fSINRMin = Float.parseFloat(vals[i++]);
		return build;
	}
}
