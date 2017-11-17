package mdtstat.struct;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.TimeHelper;
import mdtstat.Util;

public class Stat_mdt_Cell
{
	public int iCityID;
	public int iECI;
	public int ifreq;
	public int iTime;
	public long im_mdt_total;
	public int im_mdt_loc_80;
	public int im_mdt_loc_60;
	public int im_mdt_loc_40;
	public int im_mdt_loc_20;
	public int im_mdt_loc_0;
	public long logged_mdt_total;
	public int logged_mdt_loc_80;
	public int logged_mdt_loc_60;
	public int logged_mdt_loc_40;
	public int logged_mdt_loc_20;
	public int logged_mdt_loc_0;
	public long rlf_mdt_total;
	public int rlf_mdt_loc_80;
	public int rlf_mdt_loc_60;
	public int rlf_mdt_loc_40;
	public int rlf_mdt_loc_20;
	public int rlf_mdt_loc_0;
	public long rcef_mdt_total;
	public int rcef_mdt_loc_80;
	public int rcef_mdt_loc_60;
	public int rcef_mdt_loc_40;
	public int rcef_mdt_loc_20;
	public int rcef_mdt_loc_0;
	public int iMRCnt;
	public int iMRCnt_Indoor;
	public int iMRCnt_Outdoor;
	public float fRSRPValue;
	public float fRSRPValue_Indoor;
	public float fRSRPValue_Outdoor;
	public int iMRRSRQCnt;
	public int iMRRSRQCnt_Indoor;
	public int iMRRSRQCnt_Outdoor;
	public float fRSRQValue;
	public float fRSRQValue_Indoor;
	public float fRSRQValue_Outdoor;
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
	public int iMRCnt_total_0_70;
	public int iMRCnt_total_70_80;
	public int iMRCnt_total_80_90;
	public int iMRCnt_total_90_95;
	public int iMRCnt_total_100;
	public int iMRCnt_total_103;
	public int iMRCnt_total_105;
	public int iMRCnt_total_110;
	public int iMRCnt_total_113;
	public int iRSRQ_Indoor_14;
	public int iRSRQ_Outdoor_14;
	public int iRSRQ_total_14;
	public float fOverlapTotal;
	public int iOverlapMRCnt;
	public float fOverlapTotalAll;
	public int iOverlapMRCntAll;

	public static final String spliter = "\t";

	public void doFirstSample(DT_Sample_4G sample, int ifreq)
	{
		iCityID = sample.cityID;
		iECI = (int) sample.Eci;
		iTime = Util.RoundTimeForHour(sample.itime);
		this.ifreq = ifreq;
	}

	public void doSample(DT_Sample_4G sample)
	{
		doSample(sample.mrType, sample.Confidence, sample.LteScRSRP, sample.LteScRSRQ, sample.LteScSinrUL, sample.locSource, sample.samState, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleLT(DT_Sample_4G sample)
	{
		doSample(sample.mrType, sample.Confidence, sample.LteScRSRP_LT, sample.LteScRSRQ_LT, StaticConfig.Int_Abnormal, sample.locSource, sample.samState, sample.Overlap, sample.OverlapAll);
	}

	public void doSampleDX(DT_Sample_4G sample)
	{
		doSample(sample.mrType, sample.Confidence, sample.LteScRSRP_DX, sample.LteScRSRQ_DX, StaticConfig.Int_Abnormal, sample.locSource, sample.samState, sample.Overlap, sample.OverlapAll);
	}

	public void doSample(String imdtType, int confidence, int rsrp, int rsrq, int sinrul, int locSource, int samState, int Overlap, int OverlapAll)
	{
		if (rsrp >= -150 && rsrp <= -30)
		{
			iMRCnt++;

			fRSRPValue += rsrp;

			if (imdtType.equals("MDT_IMM"))
			{
				imLocCount(confidence);
			}
			else if (imdtType.equals("MDT_LOG"))
			{
				logLocCount(confidence);
			}
			totalRsrpCount(rsrp);
			if (samState == StaticConfig.ACTTYPE_IN)
			{
				inRsrpCount(rsrp);
			}
			else if (samState == StaticConfig.ACTTYPE_OUT)
			{
				outRsrpCount(rsrp);
			}
		}

		if (rsrq != -1000000)
		{
			iMRRSRQCnt++;
			fRSRQValue += rsrq;

			if (samState == StaticConfig.ACTTYPE_IN)
			{
				iMRRSRQCnt_Indoor++;
				fRSRQValue_Indoor += rsrq;

				if (rsrq > -14)
				{
					iRSRQ_Indoor_14++;
					iRSRQ_total_14++;
				}
			}
			else if (samState == StaticConfig.ACTTYPE_OUT)
			{
				iMRRSRQCnt_Outdoor++;
				fRSRQValue_Outdoor += rsrq;

				if (rsrq > -14)
				{
					iRSRQ_Outdoor_14++;
					iRSRQ_total_14++;
				}
			}
		}

		if (sinrul >= -1000 && sinrul <= 1000)
		{
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

	public void imLocCount(int confidence)
	{
		im_mdt_total++;
		if (confidence >= 80 && confidence <= 100)
		{
			im_mdt_loc_80++;
		}
		else if (confidence >= 60 && confidence < 80)
		{
			im_mdt_loc_60++;
		}
		else if (confidence >= 40 && confidence < 60)
		{
			im_mdt_loc_40++;
		}
		else if (confidence >= 20 && confidence < 40)
		{
			im_mdt_loc_20++;
		}
		else if (confidence >= 0 && confidence < 20)
		{
			im_mdt_loc_0++;
		}
	}

	public void logLocCount(int confidence)
	{
		logged_mdt_total++;
		if (confidence >= 80 && confidence <= 100)
		{
			logged_mdt_loc_80++;
		}
		else if (confidence >= 60 && confidence < 80)
		{
			logged_mdt_loc_60++;
		}
		else if (confidence >= 40 && confidence < 60)
		{
			logged_mdt_loc_40++;
		}
		else if (confidence >= 20 && confidence < 40)
		{
			logged_mdt_loc_20++;
		}
		else if (confidence >= 0 && confidence < 20)
		{
			logged_mdt_loc_0++;
		}
	}

	public void totalRsrpCount(int rsrp)
	{
		if (rsrp >= -70 && rsrp < 0)
		{
			iMRCnt_total_0_70++;
		}
		else if (rsrp >= -80 && rsrp < -70)
		{
			iMRCnt_total_70_80++;
		}
		else if (rsrp >= -90 && rsrp < -80)
		{
			iMRCnt_total_80_90++;
		}
		else if (rsrp >= -95 && rsrp < -90)
		{
			iMRCnt_total_90_95++;
		}

		if (rsrp >= -100 && rsrp < 0)
		{
			iMRCnt_total_100++;
		}
		if (rsrp >= -103 && rsrp < 0)
		{
			iMRCnt_total_103++;
		}
		if (rsrp >= -105 && rsrp < 0)
		{
			iMRCnt_total_105++;
		}
		if (rsrp >= -110 && rsrp < 0)
		{
			iMRCnt_total_110++;
		}
		if (rsrp >= -113 && rsrp < 0)
		{
			iMRCnt_total_113++;
		}
	}

	public void inRsrpCount(int rsrp)
	{
		iMRCnt_Indoor++;

		fRSRPValue_Indoor += rsrp;
		if (rsrp >= -70 && rsrp < 0)
		{
			iMRCnt_Indoor_0_70++;
		}
		else if (rsrp >= -80 && rsrp < -70)
		{
			iMRCnt_Indoor_70_80++;
		}
		else if (rsrp >= -90 && rsrp < -80)
		{
			iMRCnt_Indoor_80_90++;
		}
		else if (rsrp >= -95 && rsrp < -90)
		{
			iMRCnt_Indoor_90_95++;
		}

		if (rsrp >= -100 && rsrp < 0)
		{
			iMRCnt_Indoor_100++;
		}
		if (rsrp >= -103 && rsrp < 0)
		{
			iMRCnt_Indoor_103++;
		}
		if (rsrp >= -105 && rsrp < 0)
		{
			iMRCnt_Indoor_105++;
		}
		if (rsrp >= -110 && rsrp < 0)
		{
			iMRCnt_Indoor_110++;
		}
		if (rsrp >= -113 && rsrp < 0)
		{
			iMRCnt_Indoor_113++;
		}
	}

	public void outRsrpCount(int rsrp)
	{
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
		}
		if (rsrp >= -103 && rsrp < 0)
		{
			iMRCnt_Outdoor_103++;
		}
		if (rsrp >= -105 && rsrp < 0)
		{
			iMRCnt_Outdoor_105++;
		}
		if (rsrp >= -110 && rsrp < 0)
		{
			iMRCnt_Outdoor_110++;
		}
		if (rsrp >= -113 && rsrp < 0)
		{
			iMRCnt_Outdoor_113++;
		}
	}

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(iECI);
		bf.append(spliter);
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(iTime);
		bf.append(spliter);
		bf.append(im_mdt_total);
		bf.append(spliter);
		bf.append(im_mdt_loc_80);
		bf.append(spliter);
		bf.append(im_mdt_loc_60);
		bf.append(spliter);
		bf.append(im_mdt_loc_40);
		bf.append(spliter);
		bf.append(im_mdt_loc_20);
		bf.append(spliter);
		bf.append(im_mdt_loc_0);
		bf.append(spliter);
		bf.append(logged_mdt_total);
		bf.append(spliter);
		bf.append(logged_mdt_loc_80);
		bf.append(spliter);
		bf.append(logged_mdt_loc_60);
		bf.append(spliter);
		bf.append(logged_mdt_loc_40);
		bf.append(spliter);
		bf.append(logged_mdt_loc_20);
		bf.append(spliter);
		bf.append(logged_mdt_loc_0);
		bf.append(spliter);
		bf.append(rlf_mdt_total);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_80);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_60);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_40);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_20);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_0);
		bf.append(spliter);
		bf.append(rcef_mdt_total);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_80);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_60);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_40);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_20);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_0);
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
		bf.append(iMRCnt_total_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_total_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_total_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_total_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_total_100);
		bf.append(spliter);
		bf.append(iMRCnt_total_103);
		bf.append(spliter);
		bf.append(iMRCnt_total_105);
		bf.append(spliter);
		bf.append(iMRCnt_total_110);
		bf.append(spliter);
		bf.append(iMRCnt_total_113);
		bf.append(spliter);
		bf.append(iRSRQ_Indoor_14);
		bf.append(spliter);
		bf.append(iRSRQ_Outdoor_14);
		bf.append(spliter);
		bf.append(iRSRQ_total_14);
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
		bf.append(iECI);
		bf.append(spliter);
		bf.append(ifreq);
		bf.append(spliter);
		bf.append(TimeHelper.getRoundDayTime(iTime));
		bf.append(spliter);
		bf.append(im_mdt_total);
		bf.append(spliter);
		bf.append(im_mdt_loc_80);
		bf.append(spliter);
		bf.append(im_mdt_loc_60);
		bf.append(spliter);
		bf.append(im_mdt_loc_40);
		bf.append(spliter);
		bf.append(im_mdt_loc_20);
		bf.append(spliter);
		bf.append(im_mdt_loc_0);
		bf.append(spliter);
		bf.append(logged_mdt_total);
		bf.append(spliter);
		bf.append(logged_mdt_loc_80);
		bf.append(spliter);
		bf.append(logged_mdt_loc_60);
		bf.append(spliter);
		bf.append(logged_mdt_loc_40);
		bf.append(spliter);
		bf.append(logged_mdt_loc_20);
		bf.append(spliter);
		bf.append(logged_mdt_loc_0);
		bf.append(spliter);
		bf.append(rlf_mdt_total);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_80);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_60);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_40);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_20);
		bf.append(spliter);
		bf.append(rlf_mdt_loc_0);
		bf.append(spliter);
		bf.append(rcef_mdt_total);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_80);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_60);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_40);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_20);
		bf.append(spliter);
		bf.append(rcef_mdt_loc_0);
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
		bf.append(iMRCnt_total_0_70);
		bf.append(spliter);
		bf.append(iMRCnt_total_70_80);
		bf.append(spliter);
		bf.append(iMRCnt_total_80_90);
		bf.append(spliter);
		bf.append(iMRCnt_total_90_95);
		bf.append(spliter);
		bf.append(iMRCnt_total_100);
		bf.append(spliter);
		bf.append(iMRCnt_total_103);
		bf.append(spliter);
		bf.append(iMRCnt_total_105);
		bf.append(spliter);
		bf.append(iMRCnt_total_110);
		bf.append(spliter);
		bf.append(iMRCnt_total_113);
		bf.append(spliter);
		bf.append(iRSRQ_Indoor_14);
		bf.append(spliter);
		bf.append(iRSRQ_Outdoor_14);
		bf.append(spliter);
		bf.append(iRSRQ_total_14);
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

	public static Stat_mdt_Cell FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_mdt_Cell cell = new Stat_mdt_Cell();
		cell.iCityID = Integer.parseInt(vals[i++]);
		cell.iECI = Integer.parseInt(vals[i++]);
		cell.ifreq = Integer.parseInt(vals[i++]);
		cell.iTime = Integer.parseInt(vals[i++]);
		cell.im_mdt_total = Long.parseLong(vals[i++]);
		cell.im_mdt_loc_80 = Integer.parseInt(vals[i++]);
		cell.im_mdt_loc_60 = Integer.parseInt(vals[i++]);
		cell.im_mdt_loc_40 = Integer.parseInt(vals[i++]);
		cell.im_mdt_loc_20 = Integer.parseInt(vals[i++]);
		cell.im_mdt_loc_0 = Integer.parseInt(vals[i++]);
		cell.logged_mdt_total = Long.parseLong(vals[i++]);
		cell.logged_mdt_loc_80 = Integer.parseInt(vals[i++]);
		cell.logged_mdt_loc_60 = Integer.parseInt(vals[i++]);
		cell.logged_mdt_loc_40 = Integer.parseInt(vals[i++]);
		cell.logged_mdt_loc_20 = Integer.parseInt(vals[i++]);
		cell.logged_mdt_loc_0 = Integer.parseInt(vals[i++]);
		cell.rlf_mdt_total = Long.parseLong(vals[i++]);
		cell.rlf_mdt_loc_80 = Integer.parseInt(vals[i++]);
		cell.rlf_mdt_loc_60 = Integer.parseInt(vals[i++]);
		cell.rlf_mdt_loc_40 = Integer.parseInt(vals[i++]);
		cell.rlf_mdt_loc_20 = Integer.parseInt(vals[i++]);
		cell.rlf_mdt_loc_0 = Integer.parseInt(vals[i++]);
		cell.rcef_mdt_total = Long.parseLong(vals[i++]);
		cell.rcef_mdt_loc_80 = Integer.parseInt(vals[i++]);
		cell.rcef_mdt_loc_60 = Integer.parseInt(vals[i++]);
		cell.rcef_mdt_loc_40 = Integer.parseInt(vals[i++]);
		cell.rcef_mdt_loc_20 = Integer.parseInt(vals[i++]);
		cell.rcef_mdt_loc_0 = Integer.parseInt(vals[i++]);
		cell.iMRCnt = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Indoor = Integer.parseInt(vals[i++]);
		cell.iMRCnt_Outdoor = Integer.parseInt(vals[i++]);
		cell.iMRRSRQCnt = Integer.parseInt(vals[i++]);
		cell.iMRRSRQCnt_Indoor = Integer.parseInt(vals[i++]);
		cell.iMRRSRQCnt_Outdoor = Integer.parseInt(vals[i++]);
		cell.fRSRPValue = Float.parseFloat(vals[i++]);
		cell.fRSRPValue_Indoor = Float.parseFloat(vals[i++]);
		cell.fRSRPValue_Outdoor = Float.parseFloat(vals[i++]);
		cell.fRSRQValue = Float.parseFloat(vals[i++]);
		cell.fRSRQValue_Indoor = Float.parseFloat(vals[i++]);
		cell.fRSRQValue_Outdoor = Float.parseFloat(vals[i++]);
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
		cell.iMRCnt_total_0_70 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_70_80 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_80_90 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_90_95 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_100 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_103 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_105 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_110 = Integer.parseInt(vals[i++]);
		cell.iMRCnt_total_113 = Integer.parseInt(vals[i++]);
		cell.iRSRQ_Indoor_14 = Integer.parseInt(vals[i++]);
		cell.iRSRQ_Outdoor_14 = Integer.parseInt(vals[i++]);
		cell.iRSRQ_total_14 = Integer.parseInt(vals[i++]);
		cell.fOverlapTotal = Float.parseFloat(vals[i++]);
		cell.iOverlapMRCnt = Integer.parseInt(vals[i++]);
		cell.fOverlapTotalAll = Float.parseFloat(vals[i++]);
		cell.iOverlapMRCntAll = Integer.parseInt(vals[i++]);
		return cell;
	}
}
