package mdtstat.struct;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.TimeHelper;
import mdtstat.Util;

public class Stat_mdt_InGrid extends MdtMr
{
	public int iBuildingID;
	public int iHeight;
	public int iLongitude;
	public int iLatitude;
	public int brLongitude;
	public int brLatitude;
	public int ifreq;
	public static final String spliter = "\t";

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iBuildingID = sample.ispeed;
		iHeight = sample.imode;
		iLongitude = sample.grid.tllongitude;
		iLatitude = sample.grid.tllatitude;
		brLongitude = sample.grid.brlongitude;
		brLatitude = sample.grid.brlatitude;
		iTime = TimeHelper.getRoundDayTime(sample.itime);
		iMdtType = Util.mdtType(sample.mrType);
		this.ifreq = ifreq;
	}

	public void doSample(int rsrp, int rsrq, int sinr, int locSource, int Overlap, int OverlapAll)
	{
		if (!(rsrp >= -150 && rsrp <= -30))
			return;
		iMRCnt++;
		fRSRPValue += rsrp;

		if (rsrq != -10000)
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

	public void doSample(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.locSource, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP_DX, sample.LteScRSRQ_DX, StaticConfig.Int_Abnormal, sample.locSource, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
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

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
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
		bf.append(iBuildingID);
		bf.append(spliter);
		bf.append(iHeight);
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

	public static Stat_mdt_InGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_mdt_InGrid inGrid = new Stat_mdt_InGrid();
		inGrid.iCityID = Integer.parseInt(vals[i++]);
		inGrid.iBuildingID = Integer.parseInt(vals[i++]);
		inGrid.iHeight = Integer.parseInt(vals[i++]);
		inGrid.iLongitude = Integer.parseInt(vals[i++]);
		inGrid.iLatitude = Integer.parseInt(vals[i++]);
		inGrid.brLongitude = Integer.parseInt(vals[i++]);
		inGrid.brLatitude = Integer.parseInt(vals[i++]);
		inGrid.ifreq = Integer.parseInt(vals[i++]);
		inGrid.iMdtType = Integer.parseInt(vals[i++]);
		inGrid.iTime = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt = Integer.parseInt(vals[i++]);
		inGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);

		inGrid.fRSRPValue = Float.parseFloat(vals[i++]);
		inGrid.fRSRQValue = Float.parseFloat(vals[i++]);
		inGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
		inGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
		inGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
		inGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
		inGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		inGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		inGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		inGrid.fRSRPMax = Float.parseFloat(vals[i++]);
		inGrid.fRSRPMin = Float.parseFloat(vals[i++]);
		inGrid.fRSRQMax = Float.parseFloat(vals[i++]);
		inGrid.fRSRQMin = Float.parseFloat(vals[i++]);
		return inGrid;

	}
}
