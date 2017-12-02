package StructData;

import java.io.Console;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//import org.jruby.RubyProcess.Sys;

import base.IModel;
import jan.util.DataAdapterReader;
import jan.util.GisPos;
import jan.util.StringHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import jan.util.DataAdapterConf.ColumnInfo;
import util.DataGeter;
import util.Func;

public class SIGNAL_MR_All implements IModel
{
	public SIGNAL_MR_SC tsc;
	public short[] nccount;// 4
	public NC_LTE[] tlte;// 6
	public NC_TDS[] ttds;// 6
	public NC_GSM[] tgsm;// 6
	public NC_GSM[] tgsmall; // 6
	public SC_FRAME[] trip;// 10

	// xdr 回填信息
	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String loctp = "";// 指纹库定位fp
	public int indoor;
	public String networktype = "";
	public String lable = "";

	public int serviceType;
	public int subServiceType;

	public int moveDirect;

	// 定位添加字段2017-1-9////////////////////
	public int ispeed;
	public short imode = -1;
	public int simuLongitude;
	public int simuLatitude;
	public String UETac = "";
	// 添加字段5分钟内小区切换信息
	public String eciSwitchList = "";

	// ott/gps/fg新表 20170614
	public int locSource;// 位置来源high/mid/low
	public int samState;// int or out
	// ott/gps/fg 2017.6.19
	public int LteScRSRP_DX; // 电信主服场强
	public int LteScRSRQ_DX; // 电信主服信号质量
	public int LteScEarfcn_DX; // 电信主服频点
	public int LteScPci_DX; // 电信主服PCI
	public int LteScRSRP_LT; // 联通主服场强
	public int LteScRSRQ_LT; // 联通主服信号质量
	public int LteScEarfcn_LT; // 联通主服频点
	public int LteScPci_LT; // 联通主服PCI
	// add 场景统计标识
	public int areaId = -1;
	public int areaType = -1;
	// mdt 置信度
	public int Confidence;
	public int ConfidenceType;// 置信度类型

	public SIGNAL_MR_All()
	{
		Clear();
	}

	public void Clear()
	{
		tsc = new SIGNAL_MR_SC();
		nccount = new short[4];
		tlte = new NC_LTE[6];
		ttds = new NC_TDS[6];
		tgsm = new NC_GSM[6];
		tgsmall = new NC_GSM[6];
		trip = new SC_FRAME[10];

		for (int i = 0; i < nccount.length; i++)
			nccount[i] = 0;

		for (int i = 0; i < 6; i++)
		{
			tlte[i] = new NC_LTE();
			tlte[i].Clear();
			ttds[i] = new NC_TDS();
			ttds[i].Clear();
			tgsm[i] = new NC_GSM();
			tgsm[i].Clear();

			tgsmall[i] = new NC_GSM();
			tgsmall[i].Clear();
		}

		for (int i = 0; i < trip.length; i++)
		{
			trip[i] = new SC_FRAME();
			trip[i].Clear();
		}

		freqSet.clear();
		pos = 0;
	}

	public String GetData()
	{
		StringBuffer res = new StringBuffer();

		res.append(tsc.GetData());
		res.append(StaticConfig.DataSlipter);

		for (int data : nccount)
		{
			res.append(data);
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_LTE data : tlte)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_TDS data : ttds)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		for (NC_GSM data : tgsm)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		for (SC_FRAME data : trip)
		{
			res.append(data.GetData());
			res.append(StaticConfig.DataSlipter);
		}

		res.append(LteScRSRP_DX);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRQ_DX);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScEarfcn_DX);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPci_DX);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRP_LT);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScRSRQ_LT);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScEarfcn_LT);
		res.append(StaticConfig.DataSlipter);
		res.append(LteScPci_LT);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			for (NC_LTE data : tlte)
			{
				res.append(StaticConfig.DataSlipter);
				res.append(data.GetDataAll());
			}
			for (NC_GSM data : tgsmall)
			{
				res.append(StaticConfig.DataSlipter);
				res.append(data.GetDataAll());
			}
		}

		return StringHelper.SideTrim(res.toString(), StaticConfig.DataSlipter);
	}

	private String tmStr;

	public String GetDataEx()
	{
		tmStr = GetData();
		tmStr = tmStr.replace("" + StaticConfig.Int_Abnormal, "");
		tmStr = tmStr.replace("" + StaticConfig.Short_Abnormal, "");
		return tmStr;
	}

	public boolean FillData(Object[] args)
	{
		tsc.FillData(args);
		String values[] = (String[]) args[0];
		Integer i = (Integer) args[1];
		for (int ii = 0; ii < nccount.length; ii++)
			nccount[ii] = DataGeter.GetTinyInt(values[i + ii]);
		i += nccount.length;

		args[1] = i;
		for (int ii = 0; ii < tlte.length; ii++)
			tlte[ii].FillData(args);

		for (int ii = 0; ii < ttds.length; ii++)
			ttds[ii].FillData(args);

		for (int ii = 0; ii < tgsm.length; ii++)
			tgsm[ii].FillData(args);

		for (int ii = 0; ii < trip.length; ii++)
			trip[ii].FillData(args);
		int pos = (Integer) args[1];
		if (pos + 8 <= values.length)
		{
			LteScRSRP_DX = DataGeter.GetInt(values[pos++], 0);
			LteScRSRQ_DX = DataGeter.GetInt(values[pos++], 0);
			LteScEarfcn_DX = DataGeter.GetInt(values[pos++], 0);
			LteScPci_DX = DataGeter.GetInt(values[pos++], 0);
			LteScRSRP_LT = DataGeter.GetInt(values[pos++], 0);
			LteScRSRQ_LT = DataGeter.GetInt(values[pos++], 0);
			LteScEarfcn_LT = DataGeter.GetInt(values[pos++], 0);
			LteScPci_LT = DataGeter.GetInt(values[pos++], 0);
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			args[1] = pos;
			for (int ii = 0; ii < tlte.length; ii++)
				tlte[ii].FillDataAll(args);
			for (int ii = 0; ii < tgsmall.length; ii++)
				tgsmall[ii].FillDataAll(args);
		}

		return true;

	}

	public boolean FillIMMData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		tsc.beginTime = (int) (dataAdapterReader.GetDateValue("beginTime", null).getTime() / 1000);
		tsc.ENBId = dataAdapterReader.GetIntValue("ENBId", StaticConfig.Int_Abnormal);
		tsc.Eci = dataAdapterReader.GetIntValue("CellId", StaticConfig.Int_Abnormal);
		tsc.Earfcn = dataAdapterReader.GetIntValue("Earfcn", StaticConfig.Int_Abnormal);
		tsc.MmeGroupId = dataAdapterReader.GetIntValue("MmeGroupId", StaticConfig.Int_Abnormal);
		tsc.MmeCode = dataAdapterReader.GetIntValue("MmeCode", StaticConfig.Int_Abnormal);
		tsc.MmeUeS1apId = dataAdapterReader.GetLongValue("MmeUeS1apId", StaticConfig.Int_Abnormal);
		tsc.LteScRSRP = dataAdapterReader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		tsc.LteScRSRQ = dataAdapterReader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);
		if (tsc.LteScRSRP >= 0)
			tsc.LteScRSRP -= 141;
		if (tsc.LteScRSRQ >= 0)
			tsc.LteScRSRQ = (tsc.LteScRSRQ - 40) / 2;
		tsc.LteScEarfcn = dataAdapterReader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		tsc.LteScPci = dataAdapterReader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);
		// 置信度
		Confidence = dataAdapterReader.GetIntValue("Confidence", 0);
		if (Confidence >= 0 && Confidence <= 50)
		{
			locSource = StaticConfig.LOCTYPE_LOW;
		}
		else if (Confidence > 50 && Confidence <= 100)
		{
			locSource = StaticConfig.LOCTYPE_HIGH;
		}
		GisPos gisPos = new GisPos(dataAdapterReader.GetDoubleValue("longitude", 0), dataAdapterReader.GetDoubleValue("latitude", 0));

		/*if (gisPos.getWgLon() > 180 || gisPos.getWgLon() < 0)
		{
			tsc.longitude = 0;
		}
		if (gisPos.getWgLat() > 90 || gisPos.getWgLat() < 0)
		{
			tsc.latitude = 0;
		}*/

		tsc.longitude = (int) (gisPos.getWgLon() * 10000000D);
		tsc.latitude = (int) (gisPos.getWgLat() * 10000000D);
		tsc.EventType = "MDT_IMM";

		FillDataPciRsrp(dataAdapterReader);

		return true;
	}

	private void FillDataPciRsrp(DataAdapterReader dataAdapterReader) throws ParseException
	{
		List<NC_LTE> ncLteList = new ArrayList<>();
		for (int i = 1; i <= 6; i++)
		{
			int LteNcRSRP = dataAdapterReader.GetIntValue("LteNcRSRP" + i, StaticConfig.Int_Abnormal);
			int LteNcRSRQ = dataAdapterReader.GetIntValue("LteNcRSRQ" + i, StaticConfig.Int_Abnormal);
			int LteNcEarfcn = dataAdapterReader.GetIntValue("LteNcEarfcn" + i, StaticConfig.Int_Abnormal);
			int LteNcPci = dataAdapterReader.GetIntValue("LteNcPci" + i, StaticConfig.Int_Abnormal);
			NC_LTE nc_LTE = new StructData.NC_LTE(LteNcRSRP, LteNcRSRQ, LteNcEarfcn, LteNcPci);
			ncLteList.add(nc_LTE);
		}
		Collections.sort(ncLteList, new Comparator<NC_LTE>()
		{
			@Override
			public int compare(NC_LTE o1, NC_LTE o2)
			{
				return o2.LteNcRSRP - o1.LteNcRSRP;
			}
		});
		int cmccLteCount = 0;
		int lteCount_Freq = 0;

		StructData.NC_LTE nclte_lt = null;
		StructData.NC_LTE nclte_dx = null;

		for (int i = 0; i < ncLteList.size(); ++i)
		{
			StructData.NC_LTE ncItem = ncLteList.get(i);

			int type = Func.getFreqType(ncItem.LteNcEarfcn);
			if (type == Func.YYS_YiDong)
			{
				if (cmccLteCount < tlte.length)
				{
					tlte[cmccLteCount] = ncItem;
					cmccLteCount++;
				}
			}
			else if (type == Func.YYS_LianTong)
			{
				if (nclte_lt == null || ncItem.LteNcRSRP > nclte_lt.LteNcRSRP)
				{
					nclte_lt = ncItem;
				}
			}
			else if (type == Func.YYS_DianXin)
			{
				if (nclte_dx == null || ncItem.LteNcRSRP > nclte_dx.LteNcRSRP)
				{
					nclte_dx = ncItem;
				}
			}
		}

		// 添加联通数据
		if (nclte_lt != null && fillNclte_Freq(nclte_lt))
		{
			lteCount_Freq++;
		}
		// 添加电信数据
		if (nclte_dx != null && fillNclte_Freq(nclte_dx))
		{
			lteCount_Freq++;
		}

		nccount[0] = (byte) cmccLteCount;
		nccount[2] = (byte) (lteCount_Freq);
	}

	public boolean FillLOGData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		tsc.beginTime = (int) (dataAdapterReader.GetDateValue("beginTime", null).getTime() / 1000);
		tsc.ENBId = dataAdapterReader.GetIntValue("ENBId", StaticConfig.Int_Abnormal);
		tsc.Eci = dataAdapterReader.GetIntValue("CellId", StaticConfig.Int_Abnormal);
		tsc.IMSI = dataAdapterReader.GetLongValue("IMSI", StaticConfig.Int_Abnormal);
		tsc.MmeGroupId = dataAdapterReader.GetIntValue("MmeGroupId", StaticConfig.Int_Abnormal);
		tsc.MmeCode = dataAdapterReader.GetIntValue("MmeCode", StaticConfig.Int_Abnormal);
		tsc.MmeUeS1apId = dataAdapterReader.GetLongValue("MmeUeS1apId", StaticConfig.Int_Abnormal);
		tsc.LteScRSRP = dataAdapterReader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		tsc.LteScRSRQ = dataAdapterReader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);
		if (tsc.LteScRSRP >= 0)
			tsc.LteScRSRP -= 141;
		if (tsc.LteScRSRQ >= 0)
			tsc.LteScRSRQ = (tsc.LteScRSRQ - 40) / 2;
		GisPos gisPos = new GisPos(dataAdapterReader.GetDoubleValue("longitude", 0), dataAdapterReader.GetDoubleValue("latitude", 0));

		/*if (gisPos.getWgLon() > 180 || gisPos.getWgLon() < 0)
		{
			tsc.longitude = 0;
		}
		if (gisPos.getWgLat() > 90 || gisPos.getWgLat() < 0)
		{
			tsc.latitude = 0;
		}*/

		tsc.longitude = (int) (gisPos.getWgLon() * 10000000D);
		tsc.latitude = (int) (gisPos.getWgLat() * 10000000D);
		tsc.EventType = "MDT_LOG";
		// 置信度
		Confidence = dataAdapterReader.GetIntValue("Confidence", 0);

		if (Confidence > 0 && Confidence <= 50)
		{
			locSource = StaticConfig.LOCTYPE_LOW;
		}
		else if (Confidence > 50 && Confidence <= 100)
		{
			locSource = StaticConfig.LOCTYPE_HIGH;
		}

		FillDataPciRsrp(dataAdapterReader);
		return true;
	}

	// 需要保留异频的场强
	// 将联通和电信的测量结果保存到5个GSM邻区和4个TD邻区里面，共9个频点）
	// 联通：1600,1650,40340
	// 电信：1775,1800,1825,1850,75,100
	private Set<Integer> freqSet = new HashSet<Integer>();
	private int pos = 0;

	public boolean fillNclte_Freq(NC_LTE item)
	{
		if (item.LteNcEarfcn < 0)
		{
			return false;
		}

		if (!Func.checkFreqIsLtDx(item.LteNcEarfcn))
		{
			return false;
		}

		if (!freqSet.contains(item.LteNcEarfcn))
		{
			freqSet.add(item.LteNcEarfcn);

			if (pos >= 0 && pos <= 4)
			{
				if (item.LteNcRSRP < 0 && item.LteNcRSRP > -200)
				{
					tgsm[pos + 1].GsmNcellBcch = item.LteNcEarfcn;
					tgsm[pos + 1].GsmNcellBsic = item.LteNcPci;
					tgsm[pos + 1].GsmNcellCarrierRSSI = (item.LteNcRSRP + 200) * 1000 + (item.LteNcRSRQ + 200);
				}
			}
			else if (pos >= 5 && pos <= 8)
			{
				if (item.LteNcRSRP < 0 && item.LteNcRSRP > -200)
				{
					ttds[pos - 3].TdsNcellUarfcn = item.LteNcEarfcn;
					ttds[pos - 3].TdsCellParameterId = item.LteNcPci;
					ttds[pos - 3].TdsPccpchRSCP = (item.LteNcRSRP + 200) * 1000 + (item.LteNcRSRQ + 200);
				}
			}
			else
			{
				return false;
			}

			pos++;
		}
		return true;
	}

	public boolean FillData_UEMro(Object[] args)
	{
		String values[] = (String[]) args[0];
		int startPos = 0;// DataGeter.GetInt((String)args[1]);
		// tsc.cityID = DataGeter.GetInt(values[i++]);
		tsc.IMSI = DataGeter.GetLong(values[startPos + 5]);
		// tsc.MmeGroupId = DataGeter.GetInt(values[startPos+8]);
		// tsc.MmeCode = DataGeter.GetInt(values[startPos+9]);
		tsc.MmeUeS1apId = DataGeter.GetLong(values[startPos + 10]);
		tsc.ENBId = Integer.parseInt(values[startPos + 11], 16);
		tsc.CellId = DataGeter.GetLong(values[startPos + 12]);
		tsc.Eci = tsc.CellId;
		tsc.beginTime = (int) (DataGeter.GetLong(values[startPos + 13]) / 1000);
		tsc.beginTimems = (int) (DataGeter.GetLong(values[startPos + 13]) % 1000);
		tsc.EventType = DataGeter.GetInt(values[startPos + 14]) + "";
		// tsc.LteScPHR = DataGeter.GetInt(values[startPos+15]);
		tsc.LteScSinrUL = DataGeter.GetInt(values[startPos + 17]);
		tsc.LteScTadv = DataGeter.GetInt(values[startPos + 18]);
		tsc.LteScAOA = DataGeter.GetInt(values[startPos + 19]);
		tsc.Earfcn = DataGeter.GetInt(values[startPos + 20]);
		tsc.LteScRSRP = DataGeter.GetInt(values[startPos + 21]);
		// tsc.LteScRSRQ = DataGeter.GetInt(values[startPos+22]);

		int nbcellnum = DataGeter.GetInt(values[startPos + 23]);
		nbcellnum = nbcellnum > 6 ? 6 : nbcellnum;
		int pos = startPos + 24;
		for (int i = 0; i < nbcellnum; ++i)
		{
			tlte[i].LteNcPci = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcEarfcn = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcRSRP = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcRSRQ = DataGeter.GetInt(values[pos++]);
		}

		// 初始值 转化为 常规值
		if (tsc.LteScRSRP >= 0)
			tsc.LteScRSRP -= 141;

		if (tsc.LteScRSRQ >= 0)
		{
			tsc.LteScRSRQ = (tsc.LteScRSRQ - 40) / 2;
		}

		if (tsc.LteScSinrUL >= 0)
			tsc.LteScSinrUL -= 11;

		for (int i = 0; i < nbcellnum; ++i)
		{
			if (tlte[i].LteNcRSRP >= 0)
				tlte[i].LteNcRSRP -= 141;
			if (tlte[i].LteNcRSRQ >= 0)
				tlte[i].LteNcRSRQ = (tlte[i].LteNcRSRQ - 40) / 2;
		}

		return true;

	}

	public boolean FillData_UEMro_ShenZhen(Object[] args)
	{
		String values[] = (String[]) args[0];
		// imsi经过加密，只取后16位
		String tmImsi = values[6].length() > 15 ? values[6].substring(0, 15) : "0";
		tsc.IMSI = Long.parseLong(tmImsi, 16);
		tsc.MmeUeS1apId = DataGeter.GetLong(values[10]);
		tsc.ENBId = DataGeter.GetInt(values[11]);
		tsc.CellId = DataGeter.GetLong(values[14]);
		tsc.Eci = tsc.CellId;
		tsc.beginTime = (int) (DataGeter.GetLong(values[16]) / 1000);
		tsc.beginTimems = (int) (DataGeter.GetLong(values[16]) % 1000);
		tsc.EventType = DataGeter.GetInt(values[17]) + "";
		tsc.LteScPHR = DataGeter.GetInt(values[18]);
		tsc.LteScSinrUL = DataGeter.GetInt(values[20]);
		tsc.LteScTadv = DataGeter.GetInt(values[21]);
		tsc.LteScAOA = DataGeter.GetInt(values[22]);
		tsc.Earfcn = DataGeter.GetInt(values[23]);
		tsc.LteScRSRP = DataGeter.GetInt(values[24]);
		tsc.LteScRSRQ = DataGeter.GetInt(values[25]);

		int nbcellnum = DataGeter.GetInt(values[26]);
		nbcellnum = nbcellnum > 6 ? 6 : nbcellnum;
		int pos = 27;
		for (int i = 0; i < nbcellnum; ++i)
		{
			tlte[i].LteNcPci = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcEarfcn = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcRSRP = DataGeter.GetInt(values[pos++]);
			tlte[i].LteNcRSRQ = DataGeter.GetInt(values[pos++]);
		}

		// 初始值 转化为 常规值
		if (tsc.LteScRSRP >= 0)
			tsc.LteScRSRP -= 141;

		if (tsc.LteScRSRQ >= 0)
		{
			tsc.LteScRSRQ = (tsc.LteScRSRQ - 40) / 2;
		}

		if (tsc.LteScSinrUL >= 0)
			tsc.LteScSinrUL -= 11;

		for (int i = 0; i < nbcellnum; ++i)
		{
			if (tlte[i].LteNcRSRP >= 0)
				tlte[i].LteNcRSRP -= 141;
			if (tlte[i].LteNcRSRQ >= 0)
				tlte[i].LteNcRSRQ = (tlte[i].LteNcRSRQ - 40) / 2;
		}

		return true;

	}

	private Date tmDate = new Date();

	public boolean FillData(DataAdapterReader reader) throws ParseException
	{
		tsc.IMSI = reader.GetLongValue("IMSI", 0);
		tsc.MmeUeS1apId = reader.GetLongValue("MmeUeS1apId", -1);
		tsc.ENBId = reader.GetIntValue("ENBId", -1);
		tsc.CellId = reader.GetIntValue("CellId", -1);
		tsc.Eci = reader.GetIntValue("ECI", -1);
		if (tsc.Eci > 0)
		{
			tsc.CellId = (int) (tsc.Eci % 256);
			tsc.ENBId = (int) (tsc.Eci / 256);
		}
		else
		{
			tsc.Eci = tsc.ENBId * 256 + tsc.CellId;
		}
		tmDate = reader.GetDateValue("beginTime", tmDate);
		tsc.beginTime = (int) (tmDate.getTime() / 1000);
		tsc.beginTimems = (int) (tmDate.getTime() % 1000);
		tsc.EventType = reader.GetStrValue("EventType", "0");
		tsc.LteScPHR = reader.GetIntValue("LteScPHR", StaticConfig.Int_Abnormal);
		tsc.LteScSinrUL = reader.GetIntValue("LteScSinrUL", StaticConfig.Int_Abnormal);
		tsc.LteScTadv = reader.GetIntValue("LteScTadv", StaticConfig.Int_Abnormal);
		tsc.LteScAOA = reader.GetIntValue("LteScAOA", StaticConfig.Int_Abnormal);
		tsc.Earfcn = reader.GetIntValue("Earfcn", -1);
		tsc.LteScRSRP = reader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		tsc.LteScRSRQ = reader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);

		tsc.LteScEarfcn = reader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		tsc.LteScPci = reader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);

		int nbcellnum = reader.GetIntValue("LteNBCellNum", -1);
		nccount[0] = (short) nbcellnum;
		ColumnInfo columnInfo = reader.getColumnInfo("LteNBCellNum");
		if (columnInfo != null && nbcellnum >= 1)
		{
			int pos = columnInfo.pos + 1;
			for (int i = 0; i < nbcellnum; ++i)
			{
				tlte[i].LteNcPci = DataGeter.GetInt(reader.getStrValue(pos++, StaticConfig.Int_Abnormal + ""));
				tlte[i].LteNcEarfcn = DataGeter.GetInt(reader.getStrValue(pos++, StaticConfig.Int_Abnormal + ""));
				tlte[i].LteNcRSRP = DataGeter.GetInt(reader.getStrValue(pos++, StaticConfig.Int_Abnormal + ""));
				tlte[i].LteNcRSRQ = DataGeter.GetInt(reader.getStrValue(pos++, StaticConfig.Int_Abnormal + ""));
			}
		}

		// 初始值 转化为 常规值
		if (tsc.LteScRSRP >= 0)
			tsc.LteScRSRP -= 141;

		if (tsc.LteScRSRQ >= 0)
		{
			tsc.LteScRSRQ = (tsc.LteScRSRQ - 40) / 2;
		}

		if (tsc.LteScSinrUL >= 0)
			tsc.LteScSinrUL -= 11;

		for (int i = 0; i < nbcellnum; ++i)
		{
			if (tlte[i].LteNcRSRP >= 0)
				tlte[i].LteNcRSRP -= 141;
			if (tlte[i].LteNcRSRQ >= 0)
				tlte[i].LteNcRSRQ = (tlte[i].LteNcRSRQ - 40) / 2;
		}

		return true;

	}

	@Override
	public boolean FillData(String[] args) {
		return FillData(new Object[]{args, 0});
	}
}