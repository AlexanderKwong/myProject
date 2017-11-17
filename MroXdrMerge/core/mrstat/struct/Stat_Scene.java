package mrstat.struct;

import jan.util.TimeHelper;
import mdtstat.Util;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;

public class Stat_Scene
{
	public int iCityID;
	public int iAreaType;
	public int iAreaID;
	public int iTime;
	public int iMRCnt;
	public int iMRCnt_Indoor;
	public int iMRCnt_Outdoor;
	public int iMRRSRQCnt;
	public int iMRRSRQCnt_Indoor;
	public int iMRRSRQCnt_Outdoor;
	public int iMRSINRCnt;
	public int iMRSINRCnt_Indoor;
	public int iMRSINRCnt_Outdoor;
	public float fRSRPValue;
	public float fRSRPValue_Indoor;
	public float fRSRPValue_Outdoor;
	public float fRSRQValue;
	public float fRSRQValue_Indoor;
	public float fRSRQValue_Outdoor;
	public float fSINRValue;
	public float fSINRValue_Indoor;
	public float fSINRValue_Outdoor;
	public int iMRCnt_Indoor_0_70;
	public int iMRCnt_Indoor_70_80;
	public int iMRCnt_Indoor_80_90;
	public int iMRCnt_Indoor_90_95;
	public int iMRCnt_Indoor_100;
	public int iMRCnt_Indoor_103;
	public int iMRCnt_Indoor_105;
	public int iMRCnt_Indoor_110;
	public int iMRCnt_Indoor_113;
	public int iMRCnt_Outdoor_0_70;
	public int iMRCnt_Outdoor_70_80;
	public int iMRCnt_Outdoor_80_90;
	public int iMRCnt_Outdoor_90_95;
	public int iMRCnt_Outdoor_100;
	public int iMRCnt_Outdoor_103;
	public int iMRCnt_Outdoor_105;
	public int iMRCnt_Outdoor_110;
	public int iMRCnt_Outdoor_113;
	public int iIndoorRSRP100_SINR0;
	public int iIndoorRSRP105_SINR0;
	public int iIndoorRSRP110_SINR3;
	public int iIndoorRSRP110_SINR0;
	public int iOutdoorRSRP100_SINR0;
	public int iOutdoorRSRP105_SINR0;
	public int iOutdoorRSRP110_SINR3;
	public int iOutdoorRSRP110_SINR0;
	public int iSINR_Indoor_0;
	public int iRSRQ_Indoor_14;
	public int iSINR_Outdoor_0;
	public int iRSRQ_Outdoor_14;
	public float fOverlapTotal;
	public int iOverlapMRCnt;
	public float fOverlapTotalAll;
	public int iOverlapMRCntAll;
	public static final String spliter = "\t";

	public void doFirstSample(DT_Sample_4G sample)
	{
		iCityID = sample.cityID;
		iAreaType = sample.iAreaType;
		iAreaID = sample.iAreaID;
		iTime = Util.RoundTimeForHour(sample.itime);
	}

	public void doSample(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.Overlap, sample.OverlapAll);
	}
	
	public void doSampleLT(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP_LT, sample.LteScRSRQ_LT, StaticConfig.Int_Abnormal, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.LteScRSRP_DX, sample.LteScRSRQ_DX, StaticConfig.Int_Abnormal, sample.Overlap, sample.OverlapAll);
	}

	public void doSample(int rsrp, int rsrq, int sinrul, int Overlap, int OverlapAll)
	{
		if (rsrp >= -150 && rsrp <= -30)
		{
			iMRCnt++;

			fRSRPValue += rsrp;

			// iMRCnt_In_URI++;

			iMRCnt_Outdoor++;

			fRSRPValue_Outdoor += rsrp;

			if (rsrp >= -70 && rsrp < 0)
			{
				iMRCnt_Outdoor_0_70++;
			}
			else if (rsrp >= -80 && rsrp < -70)
			{
				iMRCnt_Outdoor_70_80++;
			}
			else if (rsrp >= -90 && rsrp < -80)
			{
				iMRCnt_Outdoor_80_90++;
			}
			else if (rsrp >= -95 && rsrp < -90)
			{
				iMRCnt_Outdoor_90_95++;
			}

			if (rsrp >= -100 && rsrp < 0)
			{
				iMRCnt_Outdoor_100++;
				if (sinrul >= 0)
				{
					iOutdoorRSRP100_SINR0++;
				}
			}
			if (rsrp >= -103 && rsrp < 0)
			{
				iMRCnt_Outdoor_103++;
			}
			if (rsrp >= -105 && rsrp < 0)
			{
				iMRCnt_Outdoor_105++;
				if (sinrul >= 0)
				{
					iOutdoorRSRP105_SINR0++;
				}
			}
			if (rsrp >= -110 && rsrp < 0)
			{
				iMRCnt_Outdoor_110++;
				if (sinrul >= 3)
				{
					iOutdoorRSRP110_SINR3++;
				}
				if (sinrul >= 0)
				{
					iOutdoorRSRP110_SINR0++;
				}
			}
			if (rsrp >= -113 && rsrp < 0)
			{
				iMRCnt_Outdoor_113++;
			}
		}

		if (rsrq >= -150 && rsrq <= -30)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;

			iMRRSRQCnt_Outdoor++;
			fRSRQValue_Outdoor += rsrq;

			if (rsrq > -14)
			{
				iRSRQ_Outdoor_14++;
			}
		}

		if (sinrul >= -1000 && sinrul <= 1000)
		{
			iMRSINRCnt++;
			fSINRValue += sinrul;

			iMRSINRCnt_Outdoor++;
			fSINRValue_Outdoor += sinrul;

			if (sinrul >= 0)
			{
				iSINR_Outdoor_0++;
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
		}
	}

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iAreaType);
		bf.append(spliter);
		bf.append(iAreaID);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRRSRQCnt_Outdoor);
		bf.append(spliter);
		bf.append(iMRSINRCnt);
		bf.append(spliter);
		bf.append(iMRSINRCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRSINRCnt_Outdoor);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_URI);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_SDK);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_HIGH);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_SIMU);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_Other);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_URI);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_SDK);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_WLAN);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_SIMU);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_Other);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRPValue_Indoor);
		bf.append(spliter);
		bf.append(fRSRPValue_Outdoor);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(fRSRQValue_Indoor);
		bf.append(spliter);
		bf.append(fRSRQValue_Outdoor);
		bf.append(spliter);
		bf.append(fSINRValue);
		bf.append(spliter);
		bf.append(fSINRValue_Indoor);
		bf.append(spliter);
		bf.append(fSINRValue_Outdoor);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_100);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_103);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_105);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_110);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_113);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_100);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_103);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_105);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_110);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_113);
		bf.append(spliter);
		bf.append(iIndoorRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iIndoorRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iIndoorRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iIndoorRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iOutdoorRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iSINR_Indoor_0);
		bf.append(spliter);
		bf.append(iRSRQ_Indoor_14);
		bf.append(spliter);
		bf.append(iSINR_Outdoor_0);
		bf.append(spliter);
		bf.append(iRSRQ_Outdoor_14);
		bf.append(spliter);
		bf.append(fOverlapTotal);
		bf.append(spliter);
		bf.append(iOverlapMRCnt);
		bf.append(spliter);
		bf.append(fOverlapTotalAll);
		bf.append(spliter);
		bf.append(iOverlapMRCntAll);
		return bf.toString();
	}
	
	public String roundDayToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iAreaType);
		bf.append(spliter);
		bf.append(iAreaID);
		bf.append(spliter);
		bf.append(TimeHelper.getRoundDayTime(iTime));
		bf.append(spliter);
		bf.append(iMRCnt);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor);
		bf.append(spliter);
		bf.append(iMRRSRQCnt);
		bf.append(spliter);
		bf.append(iMRRSRQCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRRSRQCnt_Outdoor);
		bf.append(spliter);
		bf.append(iMRSINRCnt);
		bf.append(spliter);
		bf.append(iMRSINRCnt_Indoor);
		bf.append(spliter);
		bf.append(iMRSINRCnt_Outdoor);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_URI);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_SDK);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_HIGH);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_SIMU);
		// bf.append(spliter);
		// bf.append(iMRCnt_Out_Other);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_URI);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_SDK);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_WLAN);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_SIMU);
		// bf.append(spliter);
		// bf.append(iMRCnt_In_Other);
		bf.append(spliter);
		bf.append(fRSRPValue);
		bf.append(spliter);
		bf.append(fRSRPValue_Indoor);
		bf.append(spliter);
		bf.append(fRSRPValue_Outdoor);
		bf.append(spliter);
		bf.append(fRSRQValue);
		bf.append(spliter);
		bf.append(fRSRQValue_Indoor);
		bf.append(spliter);
		bf.append(fRSRQValue_Outdoor);
		bf.append(spliter);
		bf.append(fSINRValue);
		bf.append(spliter);
		bf.append(fSINRValue_Indoor);
		bf.append(spliter);
		bf.append(fSINRValue_Outdoor);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_100);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_103);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_105);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_110);
		bf.append(spliter);
		bf.append(iMRCnt_Indoor_113);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_100);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_103);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_105);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_110);
		bf.append(spliter);
		bf.append(iMRCnt_Outdoor_113);
		bf.append(spliter);
		bf.append(iIndoorRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iIndoorRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iIndoorRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iIndoorRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP100_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP105_SINR0);
		bf.append(spliter);
		bf.append(iOutdoorRSRP110_SINR3);
		bf.append(spliter);
		bf.append(iOutdoorRSRP110_SINR0);
		bf.append(spliter);
		bf.append(iSINR_Indoor_0);
		bf.append(spliter);
		bf.append(iRSRQ_Indoor_14);
		bf.append(spliter);
		bf.append(iSINR_Outdoor_0);
		bf.append(spliter);
		bf.append(iRSRQ_Outdoor_14);
		bf.append(spliter);
		bf.append(fOverlapTotal);
		bf.append(spliter);
		bf.append(iOverlapMRCnt);
		bf.append(spliter);
		bf.append(fOverlapTotalAll);
		bf.append(spliter);
		bf.append(iOverlapMRCntAll);
		return bf.toString();
	}
	
	public static Stat_Scene FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_Scene cell = new Stat_Scene();
		cell.iCityID = Integer.parseInt(vals[i++]);
		cell.iAreaType = Integer.parseInt(vals[i++]);
		cell.iAreaID = Integer.parseInt(vals[i++]);
		cell.iTime = Integer.parseInt(vals[i++]);
		cell.iMRCnt = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor = Integer.parseInt(vals[i++]);
		cell.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		cell.iMRRSRQCnt_Indoor = Integer.parseInt(vals[i++]);
		cell.iMRRSRQCnt_Outdoor = Integer.parseInt(vals[i++]);
		cell.iMRSINRCnt = Integer.parseInt(vals[i++]);
		cell.iMRSINRCnt_Indoor = Integer.parseInt(vals[i++]);
		cell.iMRSINRCnt_Outdoor = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_Out_URI = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_Out_SDK = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_Out_HIGH = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_Out_SIMU = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_Out_Other = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_In_URI = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_In_SDK = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_In_WLAN = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_In_SIMU = Integer.parseInt(vals[i++]);
		// cell.iMRCnt_In_Other = Integer.parseInt(vals[i++]);

		cell.fRSRPValue = Float.parseFloat(vals[i++]);
		cell.fRSRPValue_Indoor = Float.parseFloat(vals[i++]);
		cell.fRSRPValue_Outdoor = Float.parseFloat(vals[i++]);
		cell.fRSRQValue = Float.parseFloat(vals[i++]);
		cell.fRSRQValue_Indoor = Float.parseFloat(vals[i++]);
		cell.fRSRQValue_Outdoor = Float.parseFloat(vals[i++]);
		cell.fSINRValue = Float.parseFloat(vals[i++]);
		cell.fSINRValue_Indoor = Float.parseFloat(vals[i++]);
		cell.fSINRValue_Outdoor = Float.parseFloat(vals[i++]);

		cell.iMRCnt_Indoor_0_70 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_70_80 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_80_90 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_90_95 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_100 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_103 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_105 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_110 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor_113 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_0_70 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_70_80 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_80_90 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_90_95 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_100 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_103 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_105 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_110 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor_113 = Integer.parseInt(vals[i++]);
		cell.iIndoorRSRP100_SINR0 = Integer.parseInt(vals[i++]);
		cell.iIndoorRSRP105_SINR0 = Integer.parseInt(vals[i++]);
		cell.iIndoorRSRP110_SINR3 = Integer.parseInt(vals[i++]);
		cell.iIndoorRSRP110_SINR0 = Integer.parseInt(vals[i++]);
		cell.iOutdoorRSRP100_SINR0 = Integer.parseInt(vals[i++]);
		cell.iOutdoorRSRP105_SINR0 = Integer.parseInt(vals[i++]);
		cell.iOutdoorRSRP110_SINR3 = Integer.parseInt(vals[i++]);
		cell.iOutdoorRSRP110_SINR0 = Integer.parseInt(vals[i++]);
		cell.iSINR_Indoor_0 = Integer.parseInt(vals[i++]);
		cell.iRSRQ_Indoor_14 = Integer.parseInt(vals[i++]);
		cell.iSINR_Outdoor_0 = Integer.parseInt(vals[i++]);
		cell.iRSRQ_Outdoor_14 = Integer.parseInt(vals[i++]);
		cell.fOverlapTotal = Float.parseFloat(vals[i++]);
		cell.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		cell.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		cell.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		return cell;
	}
}
