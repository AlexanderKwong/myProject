package mdtstat.struct;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.TimeHelper;
import mdtstat.Util;

public class Stat_mdt_Build extends MdtMr
{
	public int iBuildingID;
	public int iHeight;
	public int ifreq;
	public static final String spliter = "\t";

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ispeed;
		iHeight = sample.imode;
		iTime = Util.RoundTimeForHour(sample.itime);
		iMdtType = Util.mdtType(sample.mrType);
		this.ifreq = ifreq;
	}

	public void doSample(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.locSource, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP_LT, sample.LteScRSRQ_LT, StaticConfig.Int_Abnormal, sample.locSource, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP_DX, sample.LteScRSRQ_DX, StaticConfig.Int_Abnormal, sample.locSource, sample.Overlap, sample.OverlapAll);
	}

	public void doSample(int rsrp, int rsrq, int sinrul, int locSource, int Overlap, int OverlapAll)
	{
		if (!(rsrp >= -150 && rsrp <= -30))
		{
			return;
		}
		iMRCnt++;
		fRSRPValue += rsrp;
		if (rsrq != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;
		}
		if (rsrp >= -95)
		{
			iMRCnt_95++;
		}
		if (rsrp >= -100)
		{
			iMRCnt_100++;
		}
		if (rsrp >= -103)
		{
			iMRCnt_103++;
		}
		if (rsrp >= -105)
		{
			iMRCnt_105++;
		}
		if (rsrp >= -110)
		{
			iMRCnt_110++;
		}
		if (rsrp >= -113)
		{
			iMRCnt_113++;
		}
		if (rsrp >= -128)
		{
			iMRCnt_128++;
		}

		if (rsrq >= -14)
		{
			iRSRQ_14++;
		}

		fOverlapTotal += Overlap;
		if (Overlap >= 4)
		{
			iOverlapMRCnt++;
		}
		fOverlapTotalAll += OverlapAll;
		if (OverlapAll >= 4)
		{
			iOverlapMRCntAll++;
		}
		fRSRPMax = getMax(fRSRPMax, rsrp);
		fRSRPMin = getMin(fRSRPMin, rsrp);
		fRSRQMax = getMax(fRSRQMax, rsrq);
		fRSRQMin = getMin(fRSRQMin, rsrq);
	}

	private float getMax(float valueMax, int value)
	{
		if (valueMax == StaticConfig.Int_Abnormal || valueMax < value)
		{
			return value;
		}
		return valueMax;
	}

	private float getMin(float valueMin, int value)
	{
		if (value == StaticConfig.Int_Abnormal)
			return valueMin;
		if (valueMin == StaticConfig.Int_Abnormal || valueMin > value)
		{
			return value;
		}
		return valueMin;
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
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(iMdtType);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRQValue);
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
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(iMdtType);
		bf.append(spliter);
		bf.append(TimeHelper.getRoundDayTime(iTime));
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRQValue);
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
		return bf.toString();
	}

	public static Stat_mdt_Build FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_mdt_Build build = new Stat_mdt_Build();
		build.iCityID = Integer.parseInt(vals[i++]);
		build.iBuildingID = Integer.parseInt(vals[i++]);
		build.iHeight = Integer.parseInt(vals[i++]);
		build.ifreq = Integer.parseInt(vals[i++]);
		build.iMdtType = Integer.parseInt(vals[i++]);
		build.iTime = Integer.parseInt(vals[i++]);
		build.iMRCnt = Integer.parseInt(vals[i++]);
		build.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		build.fRSRPValue = Float.parseFloat(vals[i++]);
		build.fRSRQValue = Float.parseFloat(vals[i++]);
		build.iMRCnt_95 = Integer.parseInt(vals[i++]);
		build.iMRCnt_100 = Integer.parseInt(vals[i++]);
		build.iMRCnt_103 = Integer.parseInt(vals[i++]);
		build.iMRCnt_105 = Integer.parseInt(vals[i++]);
		build.iMRCnt_110 = Integer.parseInt(vals[i++]);
		build.iMRCnt_113 = Integer.parseInt(vals[i++]);
		build.iMRCnt_128 = Integer.parseInt(vals[i++]);
		build.iRSRQ_14 = Integer.parseInt(vals[i++]);
		build.fOverlapTotal = Float.parseFloat(vals[i++]);
		build.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		build.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		build.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		build.fRSRPMax = Float.parseFloat(vals[i++]);
		build.fRSRPMin = Float.parseFloat(vals[i++]);
		build.fRSRQMax = Float.parseFloat(vals[i++]);
		build.fRSRQMin = Float.parseFloat(vals[i++]);
		return build;
	}

}
