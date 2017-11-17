package mdtstat.struct;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.TimeHelper;
import mdtstat.Util;

public class Stat_mdt_Out_CellGrid extends MdtMr
{
	public int iECI;
	public int iLongitude;
	public int iLatitude;
	public int brLongitude;
	public int brLatitude;
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
		bf.append(iECI);
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
		bf.append(iECI);
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

	public void doFirstSample(DT_Sample_4G sample)
	{
		iCityID = sample.cityID;
		iLongitude = sample.grid.tllongitude;
		iLatitude = sample.grid.tllatitude;
		brLongitude = sample.grid.brlongitude;
		brLatitude = sample.grid.brlatitude;
		iECI = (int) sample.Eci;
		iTime = Util.RoundTimeForHour(sample.itime);
		iMdtType = Util.mdtType(sample.mrType);
	}

	public void doSample(DT_Sample_4G sample)
	{
		if (!(sample.LteScRSRP >= -150 && sample.LteScRSRP <= -30))
			return;
		iMRCnt++;
		fRSRPValue += sample.LteScRSRP;

		if (sample.LteScRSRQ != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += sample.LteScRSRQ;
		}
		if (sample.LteScRSRP >= -95)
		{
			iMRCnt_95++;
		}
		if (sample.LteScRSRP >= -100)
		{
			iMRCnt_100++;
		}
		if (sample.LteScRSRP >= -103)
		{
			iMRCnt_103++;
		}
		if (sample.LteScRSRP >= -105)
		{
			iMRCnt_105++;
		}
		if (sample.LteScRSRP >= -110)
		{
			iMRCnt_110++;
		}
		if (sample.LteScRSRP >= -113)
		{
			iMRCnt_113++;
		}
		if (sample.LteScRSRP >= -128)
		{
			iMRCnt_128++;
		}
		if (sample.LteScRSRQ >= -14)
		{
			iRSRQ_14++;
		}

		fOverlapTotal += sample.Overlap;
		if (sample.Overlap >= 4)
		{
			iOverlapMRCnt++;
		}
		fOverlapTotalAll += sample.OverlapAll;
		if (sample.OverlapAll >= 4)
		{
			iOverlapMRCntAll++;
		}
		fRSRPMax = getMax(fRSRPMax, sample.LteScRSRP);
		fRSRPMin = getMin(fRSRPMin, sample.LteScRSRP);
		fRSRQMax = getMax(fRSRQMax, sample.LteScRSRQ);
		fRSRQMin = getMin(fRSRQMin, sample.LteScRSRQ);
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

	public static Stat_mdt_Out_CellGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_mdt_Out_CellGrid outcellGrid = new Stat_mdt_Out_CellGrid();
		outcellGrid.iCityID = Integer.parseInt(vals[i++]);
		outcellGrid.iLongitude = Integer.parseInt(vals[i++]);
		outcellGrid.iLatitude = Integer.parseInt(vals[i++]);
		outcellGrid.brLongitude = Integer.parseInt(vals[i++]);
		outcellGrid.brLatitude = Integer.parseInt(vals[i++]);
		outcellGrid.iECI = Integer.parseInt(vals[i++]);
		outcellGrid.iMdtType = Integer.parseInt(vals[i++]);
		outcellGrid.iTime = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt = Integer.parseInt(vals[i++]);
		outcellGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		outcellGrid.fRSRPValue = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRQValue = Float.parseFloat(vals[i++]);
		outcellGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
		outcellGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
		outcellGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
		outcellGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		outcellGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		outcellGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		outcellGrid.fRSRPMax = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRPMin = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRQMax = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRQMin = Float.parseFloat(vals[i++]);
		return outcellGrid;
	}
}
