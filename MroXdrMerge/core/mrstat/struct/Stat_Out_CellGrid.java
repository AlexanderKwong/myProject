package mrstat.struct;

import mdtstat.Util;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.TimeHelper;

public class Stat_Out_CellGrid extends Stat_OutGrid
{
	public int iECI;
	// public int iASNei_MRCnt;
	// public float fASNei_RSRPValue;

	public static final String spliter = "\t";

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(tllongitude);
		bf.append(spliter);
		bf.append(tllatitude);
		bf.append(spliter);
		bf.append(brlongitude);
		bf.append(spliter);
		bf.append(brlatitude);
		bf.append(spliter);
		bf.append(iECI);
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
		bf.append(tllongitude);
		bf.append(spliter);
		bf.append(tllatitude);
		bf.append(spliter);
		bf.append(brlongitude);
		bf.append(spliter);
		bf.append(brlatitude);
		bf.append(spliter);
		bf.append(iECI);
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

	public void doFirstSample(DT_Sample_4G sample)
	{
		iCityID = sample.cityID;
		tllongitude = sample.grid.tllongitude;
		tllatitude = sample.grid.tllatitude;
		brlongitude = sample.grid.brlongitude;
		brlatitude = sample.grid.brlatitude;
		iECI = (int) sample.Eci;
		iTime = Util.RoundTimeForHour(sample.itime);
	}


	public static Stat_Out_CellGrid FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_Out_CellGrid outcellGrid = new Stat_Out_CellGrid();
		outcellGrid.iCityID = Integer.parseInt(vals[i++]);
		outcellGrid.tllongitude = Integer.parseInt(vals[i++]);
		outcellGrid.tllatitude = Integer.parseInt(vals[i++]);
		outcellGrid.brlongitude = Integer.parseInt(vals[i++]);
		outcellGrid.brlatitude = Integer.parseInt(vals[i++]);
		outcellGrid.iECI = Integer.parseInt(vals[i++]);
		outcellGrid.iTime = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt = Integer.parseInt(vals[i++]);
		outcellGrid.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		outcellGrid.iMRSINRCnt = Integer.parseInt(vals[i++]);
		outcellGrid.fRSRPValue = Double.parseDouble(vals[i++]);
		outcellGrid.fRSRQValue = Double.parseDouble(vals[i++]);
		outcellGrid.fSINRValue = Double.parseDouble(vals[i++]);
		outcellGrid.iMRCnt_95 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_100 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_103 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_105 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_110 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_113 = Integer.parseInt(vals[i++]);
		outcellGrid.iMRCnt_128 = Integer.parseInt(vals[i++]);
		outcellGrid.iRSRP100_SINR0 = Integer.parseInt(vals[i++]);
		outcellGrid.iRSRP105_SINR0 = Integer.parseInt(vals[i++]);
		outcellGrid.iRSRP110_SINR3 = Integer.parseInt(vals[i++]);
		outcellGrid.iRSRP110_SINR0 = Integer.parseInt(vals[i++]);
		outcellGrid.iSINR_0 = Integer.parseInt(vals[i++]);
		outcellGrid.iRSRQ_14 = Integer.parseInt(vals[i++]);
		outcellGrid.fOverlapTotal = Float.parseFloat(vals[i++]);
		outcellGrid.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		outcellGrid.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		outcellGrid.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		outcellGrid.fRSRPMax = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRPMin = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRQMax = Float.parseFloat(vals[i++]);
		outcellGrid.fRSRQMin = Float.parseFloat(vals[i++]);
		outcellGrid.fSINRMax = Float.parseFloat(vals[i++]);
		outcellGrid.fSINRMin = Float.parseFloat(vals[i++]);

		return outcellGrid;
	}
}
