package mdtstat.struct;

import mdtstat.Util;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.TimeHelper;

public class Stat_mdt_OutGrid extends MdtMr
{
	public int iLongitude;
	public int iLatitude;
	public int brLongitude;
	public int brLatitude;
	public int ifreq;
	public static final String spliter = "\t";

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iLongitude);
		bf.append(spliter);
		bf.append(iLatitude);
		bf.append(spliter);
		bf.append(brLongitude);
		bf.append(spliter);
		bf.append(brLatitude);
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
		bf.append(iLongitude);
		bf.append(spliter);
		bf.append(iLatitude);
		bf.append(spliter);
		bf.append(brLongitude);
		bf.append(spliter);
		bf.append(brLatitude);
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

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iLongitude = sample.grid.tllongitude;
		iLatitude = sample.grid.tllatitude;
		brLongitude = sample.grid.brlongitude;
		brLatitude = sample.grid.brlatitude;
		iTime = Util.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
		iMdtType = Util.mdtType(sample.mrType);
	}

	public void doSample(int rsrp, int rsrq, int sinr, int locSource, int Overlap, int OverlapAll)
	{
		if (!(rsrp >= -150 && rsrp <= -30))
			return;
		iMRCnt++;
		fRSRPValue += rsrp; // 总的MR的RSRP值，按服务小区计算

		if (rsrq != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;
		}
		if (rsrp >= -95)
		{
			iMRCnt_95++; // 大于等于-95dB的采样点数，按服务小区计算
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
		if (rsrq > -14)
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

	public static Stat_mdt_OutGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_mdt_OutGrid outGrid = new Stat_mdt_OutGrid();
		outGrid.iCityID = Integer.parseInt(vals[i++]);
		outGrid.iLongitude = Integer.parseInt(vals[i++]);
		outGrid.iLatitude = Integer.parseInt(vals[i++]);
		outGrid.brLongitude = Integer.parseInt(vals[i++]);
		outGrid.brLatitude = Integer.parseInt(vals[i++]);
		outGrid.ifreq = Integer.parseInt(vals[i++]);
		outGrid.iMdtType = Integer.parseInt(vals[i++]);
		outGrid.iTime = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt = Integer.parseInt(vals[i++]);
		outGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		outGrid.fRSRPValue = Float.parseFloat(vals[i++]);
		outGrid.fRSRQValue = Float.parseFloat(vals[i++]);
		outGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
		outGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
		outGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
		outGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
		outGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		outGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		outGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		outGrid.fRSRPMax = Float.parseFloat(vals[i++]);
		outGrid.fRSRPMin = Float.parseFloat(vals[i++]);
		outGrid.fRSRQMax = Float.parseFloat(vals[i++]);
		outGrid.fRSRQMin = Float.parseFloat(vals[i++]);
		return outGrid;

	}
}
