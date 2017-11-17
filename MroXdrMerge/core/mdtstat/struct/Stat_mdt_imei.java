package mdtstat.struct;

import mdtstat.Util;
import StructData.DT_Sample_4G;
import jan.util.TimeHelper;

public class Stat_mdt_imei
{
	public int iCityID;
	public int IMEI_TAC;
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
	public int rcef__mdt_loc_80;
	public int rcef__mdt_loc_60;
	public int rcef__mdt_loc_40;
	public int rcef__mdt_loc_20;
	public int rcef__mdt_loc_0;

	public static final String spliter = "\t";

	public String toLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(IMEI_TAC);
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
		bf.append(iTime);
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
		bf.append(rcef__mdt_loc_80);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_60);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_40);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_20);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_0);
		return bf.toString();
	}

	public String roundDayToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(iCityID);
		bf.append(spliter);
		bf.append(IMEI_TAC);
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
		bf.append(iTime);
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
		bf.append(rcef__mdt_loc_80);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_60);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_40);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_20);
		bf.append(spliter);
		bf.append(rcef__mdt_loc_0);
		return bf.toString();
	}

	public void doFirstSample(DT_Sample_4G sample)
	{
		iCityID = sample.cityID;
		IMEI_TAC = sample.imeiTac;
		iTime = Util.RoundTimeForHour(sample.itime);
	}

	public void doSample(DT_Sample_4G sample)
	{
		if (sample.mrType.equals("MDT_IMM"))
		{
			im_mdt_total++;
			if (!(sample.Confidence >= 0 && sample.Confidence <= 100))
			{
				return;
			}
			if (sample.Confidence >= 80 && sample.Confidence <= 100)
			{
				im_mdt_loc_80++;
			}
			else if (sample.Confidence >= 60 && sample.Confidence <= 80)
			{
				im_mdt_loc_60++;
			}
			else if (sample.Confidence >= 40 && sample.Confidence <= 60)
			{
				im_mdt_loc_40++;
			}
			else if (sample.Confidence >= 20 && sample.Confidence <= 40)
			{
				im_mdt_loc_20++;
			}
			else if (sample.Confidence >= 0 && sample.Confidence <= 20)
			{
				im_mdt_loc_0++;
			}
		}
		else if (sample.mrType.equals("MDT_LOG"))
		{
			logged_mdt_total++;
			if (!(sample.Confidence >= 0 && sample.Confidence <= 100))
			{
				return;
			}
			else if (sample.Confidence >= 80 && sample.Confidence <= 100)
			{
				logged_mdt_loc_80++;
			}
			else if (sample.Confidence >= 60 && sample.Confidence <= 80)
			{
				logged_mdt_loc_60++;
			}
			else if (sample.Confidence >= 40 && sample.Confidence <= 60)
			{
				logged_mdt_loc_40++;
			}
			else if (sample.Confidence >= 20 && sample.Confidence <= 40)
			{
				logged_mdt_loc_20++;
			}
			else if (sample.Confidence >= 0 && sample.Confidence <= 20)
			{
				logged_mdt_loc_0++;
			}
		}
	}

	public static Stat_mdt_imei FillData(String[] vals, int pos)
	{
		int i = pos;
		Stat_mdt_imei imei = new Stat_mdt_imei();
		imei.iCityID = Integer.parseInt(vals[i++]);
		imei.IMEI_TAC = Integer.parseInt(vals[i++]);
		imei.iTime = Integer.parseInt(vals[i++]);
		imei.im_mdt_total = Long.parseLong(vals[i++]);
		imei.im_mdt_loc_80 = Integer.parseInt(vals[i++]);
		imei.im_mdt_loc_60 = Integer.parseInt(vals[i++]);
		imei.im_mdt_loc_40 = Integer.parseInt(vals[i++]);
		imei.im_mdt_loc_20 = Integer.parseInt(vals[i++]);
		imei.im_mdt_loc_0 = Integer.parseInt(vals[i++]);
		imei.logged_mdt_total = Long.parseLong(vals[i++]);
		imei.logged_mdt_loc_80 = Integer.parseInt(vals[i++]);
		imei.logged_mdt_loc_60 = Integer.parseInt(vals[i++]);
		imei.logged_mdt_loc_40 = Integer.parseInt(vals[i++]);
		imei.logged_mdt_loc_20 = Integer.parseInt(vals[i++]);
		imei.logged_mdt_loc_0 = Integer.parseInt(vals[i++]);
		imei.rlf_mdt_total = Long.parseLong(vals[i++]);
		imei.rlf_mdt_loc_80 = Integer.parseInt(vals[i++]);
		imei.rlf_mdt_loc_60 = Integer.parseInt(vals[i++]);
		imei.rlf_mdt_loc_40 = Integer.parseInt(vals[i++]);
		imei.rlf_mdt_loc_20 = Integer.parseInt(vals[i++]);
		imei.rlf_mdt_loc_0 = Integer.parseInt(vals[i++]);
		imei.rcef_mdt_total = Long.parseLong(vals[i++]);
		imei.rcef__mdt_loc_80 = Integer.parseInt(vals[i++]);
		imei.rcef__mdt_loc_60 = Integer.parseInt(vals[i++]);
		imei.rcef__mdt_loc_40 = Integer.parseInt(vals[i++]);
		imei.rcef__mdt_loc_20 = Integer.parseInt(vals[i++]);
		imei.rcef__mdt_loc_0 = Integer.parseInt(vals[i++]);
		return imei;
	}

}
