package StructData;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import jan.util.DataAdapterReader;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.DataGeter;

public class MroOrigDataMT
{
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

	public Date beginTime;// yyyy-MM-dd HH:mm:ss.000
	public int ENBId;
	public String UserLabel;
	public int CellId;
	public int Earfcn;
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public String EventType;
	public int LteScRSRP;
	public int LteNcRSRP;
	public int LteScRSRQ;
	public int LteNcRSRQ;
	public int LteScEarfcn;
	public int LteScPci;
	public int LteNcEarfcn;
	public int LteNcPci;
	public int GsmNcellCarrierRSSI;
	public int GsmNcellBcch;
	public int GsmNcellNcc;
	public int GsmNcellBcc;
	public int TdsPccpchRSCP;
	public int TdsNcellUarfcn;
	public int TdsCellParameterId;
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public int LteScRIP;
	public int LteScSinrUL;
	public int[] LteScPlrULQci;
	public int[] LteScPlrDLQci;
	public int LteScRI1;
	public int LteScRI2;
	public int LteScRI4;
	public int LteScRI8;
	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	// neimeng
	public int GsmNcellLac;
	public int GsmNcellCi;
	public int LteNcEnodeb;
	public int LteNcCid;

	private String tmStr = "";

	public MroOrigDataMT()
	{
		beginTime = new Date();
		ENBId = StaticConfig.Int_Abnormal;
		UserLabel = "";
		CellId = StaticConfig.Int_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		SubFrameNbr = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		EventType = "";
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteNcRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		LteNcRSRQ = StaticConfig.Int_Abnormal;
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteNcEarfcn = StaticConfig.Int_Abnormal;
		LteNcPci = StaticConfig.Int_Abnormal;
		GsmNcellCarrierRSSI = StaticConfig.Int_Abnormal;
		GsmNcellBcch = StaticConfig.Int_Abnormal;
		GsmNcellNcc = StaticConfig.Int_Abnormal;
		GsmNcellBcc = StaticConfig.Int_Abnormal;
		TdsPccpchRSCP = StaticConfig.Int_Abnormal;
		TdsNcellUarfcn = StaticConfig.Int_Abnormal;
		TdsCellParameterId = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScRIP = StaticConfig.Int_Abnormal;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LteScPlrULQci = new int[9];
		for (int i = 0; i < LteScPlrULQci.length; ++i)
		{
			LteScPlrULQci[i] = StaticConfig.Int_Abnormal;
		}

		LteScPlrDLQci = new int[9];
		for (int i = 0; i < LteScPlrDLQci.length; ++i)
		{
			LteScPlrDLQci[i] = StaticConfig.Int_Abnormal;
		}

		LteScRI1 = StaticConfig.Int_Abnormal;
		LteScRI2 = StaticConfig.Int_Abnormal;
		LteScRI4 = StaticConfig.Int_Abnormal;
		LteScRI8 = StaticConfig.Int_Abnormal;
		LteScPUSCHPRBNum = StaticConfig.Int_Abnormal;
		LteScPDSCHPRBNum = StaticConfig.Int_Abnormal;
		LteSceNBRxTxTimeDiff = StaticConfig.Int_Abnormal;

		GsmNcellLac = StaticConfig.Int_Abnormal;
		GsmNcellCi = StaticConfig.Int_Abnormal;
		LteNcEnodeb = StaticConfig.Int_Abnormal;
		LteNcCid = StaticConfig.Int_Abnormal;
	}

	public boolean FillData(String[] values, int startPos)
	{
		try
		{
			int i = startPos;

			beginTime = format.parse(values[i++]);
			ENBId = DataGeter.GetInt(values[i++]);
			UserLabel = values[i++];
			CellId = DataGeter.GetInt(values[i++]);
			if (CellId > 256)
			{
				CellId = CellId % 256;
			}
			Earfcn = DataGeter.GetInt(values[i++]);
			SubFrameNbr = DataGeter.GetInt(values[i++]);
			MmeCode = DataGeter.GetInt(values[i++]);
			MmeGroupId = DataGeter.GetInt(values[i++]);
			MmeUeS1apId = DataGeter.GetLong(values[i++]);
			Weight = DataGeter.GetInt(values[i++]);
			EventType = values[i++];
			LteScRSRP = DataGeter.GetInt(values[i++]);
			LteNcRSRP = DataGeter.GetInt(values[i++]);
			LteScRSRQ = DataGeter.GetInt(values[i++]);
			LteNcRSRQ = DataGeter.GetInt(values[i++]);
			LteScEarfcn = DataGeter.GetInt(values[i++]);
			LteScPci = DataGeter.GetInt(values[i++]);
			LteNcEarfcn = DataGeter.GetInt(values[i++]);
			LteNcPci = DataGeter.GetInt(values[i++]);
			GsmNcellCarrierRSSI = DataGeter.GetInt(values[i++]);
			GsmNcellBcch = DataGeter.GetInt(values[i++]);
			GsmNcellNcc = DataGeter.GetInt(values[i++]);
			GsmNcellBcc = DataGeter.GetInt(values[i++]);
			TdsPccpchRSCP = DataGeter.GetInt(values[i++]);
			TdsNcellUarfcn = DataGeter.GetInt(values[i++]);
			TdsCellParameterId = DataGeter.GetInt(values[i++]);
			LteScBSR = DataGeter.GetInt(values[i++]);
			LteScRTTD = DataGeter.GetInt(values[i++]);
			LteScTadv = DataGeter.GetInt(values[i++]);
			LteScAOA = DataGeter.GetInt(values[i++]);
			LteScPHR = DataGeter.GetInt(values[i++]);
			LteScRIP = DataGeter.GetInt(values[i++]);
			LteScSinrUL = DataGeter.GetInt(values[i++]);

			for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
			{
				i++;
				LteScPlrULQci[ii] = 0;
				// LteScPlrULQci[ii] = DataConverter.GetInt(values[i++]);
			}

			for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
			{
				i++;
				LteScPlrDLQci[ii] = 0;
				// LteScPlrDLQci[ii] = DataConverter.GetInt(values[i++]);
			}

			LteScRI1 = DataGeter.GetInt(values[i++]);
			LteScRI2 = DataGeter.GetInt(values[i++]);
			LteScRI4 = DataGeter.GetInt(values[i++]);
			LteScRI8 = DataGeter.GetInt(values[i++]);
			LteScPUSCHPRBNum = DataGeter.GetInt(values[i++]);
			LteScPDSCHPRBNum = DataGeter.GetInt(values[i++]);
			LteSceNBRxTxTimeDiff = DataGeter.GetInt(values[i++]);

			// 初始值 转化为 常规值
			if (LteNcRSRP > 0)
				LteNcRSRP -= 141;
			if (LteNcRSRQ >= 0)
				LteNcRSRQ = (LteNcRSRQ - 40) / 2;
			if (GsmNcellCarrierRSSI >= 0)
				GsmNcellCarrierRSSI -= 101;
			if (TdsPccpchRSCP >= 0)
				TdsPccpchRSCP -= 116;

			if (LteScRSRP >= 0)
				LteScRSRP -= 141;
			if (LteScRSRQ >= 0)
				LteScRSRQ = (LteScRSRQ - 40) / 2;
			if (LteScSinrUL >= 0)
				LteScSinrUL -= 11;
		}
		catch (Exception e)
		{
			return false;
		}

		return true;
	}

	public boolean FillData(DataAdapterReader reader) throws ParseException
	{
		beginTime = reader.GetDateValue("beginTime", null);
		ENBId = reader.GetIntValue("ENBId", -1);
		UserLabel = reader.GetStrValue("UserLabel", "");
		// CellId = reader.GetIntValue("CellId", -1);
		tmStr = reader.GetStrValue("CellId", "0");
		if (tmStr.indexOf(":") > 0)
		{
			CellId = DataGeter.GetInt(tmStr.substring(0, tmStr.indexOf(":")));
		}
		else if (tmStr.indexOf("-") > 0)
		{
			CellId = DataGeter.GetInt(tmStr.substring(tmStr.indexOf("-") + 1));
		}
		else
		{
			CellId = DataGeter.GetInt(tmStr);
		}

		if (CellId > 256)
		{
			CellId = CellId % 256;
		}
		Earfcn = reader.GetIntValue("Earfcn", StaticConfig.Int_Abnormal);
		SubFrameNbr = reader.GetIntValue("SubFrameNbr", StaticConfig.Int_Abnormal);
		MmeCode = reader.GetIntValue("MmeCode", StaticConfig.Int_Abnormal);
		MmeGroupId = reader.GetIntValue("MmeGroupId", StaticConfig.Int_Abnormal);
		MmeUeS1apId = reader.GetLongValue("MmeUeS1apId", StaticConfig.Int_Abnormal);
		Weight = reader.GetIntValue("Weight", StaticConfig.Int_Abnormal);
		EventType = reader.GetStrValue("EventType", "");
		LteScRSRP = reader.GetIntValue("LteScRSRP", StaticConfig.Int_Abnormal);
		LteNcRSRP = reader.GetIntValue("LteNcRSRP", StaticConfig.Int_Abnormal);
		LteScRSRQ = reader.GetIntValue("LteScRSRQ", StaticConfig.Int_Abnormal);
		LteNcRSRQ = reader.GetIntValue("LteNcRSRQ", StaticConfig.Int_Abnormal);
		LteScEarfcn = reader.GetIntValue("LteScEarfcn", StaticConfig.Int_Abnormal);
		LteScPci = reader.GetIntValue("LteScPci", StaticConfig.Int_Abnormal);
		LteNcEarfcn = reader.GetIntValue("LteNcEarfcn", StaticConfig.Int_Abnormal);
		LteNcPci = reader.GetIntValue("LteNcPci", StaticConfig.Int_Abnormal);
		GsmNcellCarrierRSSI = reader.GetIntValue("GsmNcellCarrierRSSI", StaticConfig.Int_Abnormal);
		GsmNcellBcch = reader.GetIntValue("GsmNcellBcch", StaticConfig.Int_Abnormal);
		GsmNcellNcc = reader.GetIntValue("GsmNcellNcc", StaticConfig.Int_Abnormal);
		GsmNcellBcc = reader.GetIntValue("GsmNcellBcc", StaticConfig.Int_Abnormal);
		TdsPccpchRSCP = reader.GetIntValue("TdsPccpchRSCP", StaticConfig.Int_Abnormal);
		TdsNcellUarfcn = reader.GetIntValue("TdsNcellUarfcn", StaticConfig.Int_Abnormal);
		TdsCellParameterId = reader.GetIntValue("TdsCellParameterId", StaticConfig.Int_Abnormal);
		LteScBSR = reader.GetIntValue("LteScBSR", StaticConfig.Int_Abnormal);
		LteScRTTD = reader.GetIntValue("LteScRTTD", StaticConfig.Int_Abnormal);
		LteScTadv = reader.GetIntValue("LteScTadv", StaticConfig.Int_Abnormal);
		LteScAOA = reader.GetIntValue("LteScAOA", StaticConfig.Int_Abnormal);
		LteScPHR = reader.GetIntValue("LteScPHR", StaticConfig.Int_Abnormal);
		LteScRIP = reader.GetIntValue("LteScRIP", StaticConfig.Int_Abnormal);
		LteScSinrUL = reader.GetIntValue("LteScSinrUL", StaticConfig.Int_Abnormal);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			GsmNcellLac = reader.GetIntValue("GsmNcellLac", StaticConfig.Int_Abnormal);
			GsmNcellCi = reader.GetIntValue("GsmNcellCi", StaticConfig.Int_Abnormal);
			LteNcEnodeb = reader.GetIntValue("LteNcEnodeb", StaticConfig.Int_Abnormal);
			LteNcCid = reader.GetIntValue("LteNcCid", StaticConfig.Int_Abnormal);
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.MroDetail))
		{

			for (int ii = 0; ii < LteScPlrULQci.length; ++ii)
			{
				LteScPlrULQci[ii] = reader.GetIntValue("LteScPlrULQci" + (ii + 1), StaticConfig.Int_Abnormal);
			}

			for (int ii = 0; ii < LteScPlrDLQci.length; ++ii)
			{
				LteScPlrDLQci[ii] = reader.GetIntValue("LteScPlrDLQci" + (ii + 1), StaticConfig.Int_Abnormal);
			}
//唐总要求暂时屏蔽
//			LteScRI1 = reader.GetIntValue("LteScRI1", StaticConfig.Int_Abnormal);
//			LteScRI2 = reader.GetIntValue("LteScRI2", StaticConfig.Int_Abnormal);
//			LteScRI4 = reader.GetIntValue("LteScRI4", StaticConfig.Int_Abnormal);
//			LteScRI8 = reader.GetIntValue("LteScRI8", StaticConfig.Int_Abnormal);
//			LteScPUSCHPRBNum = reader.GetIntValue("LteScPUSCHPRBNum", StaticConfig.Int_Abnormal);
//			LteScPDSCHPRBNum = reader.GetIntValue("LteScPDSCHPRBNum", StaticConfig.Int_Abnormal);
//			LteSceNBRxTxTimeDiff = reader.GetIntValue("LteSceNBRxTxTimeDiff", StaticConfig.Int_Abnormal);
		}

		// 初始值 转化为 常规值
		if (LteNcRSRP > 0)
			LteNcRSRP -= 141;
		if (LteNcRSRQ >= 0)
			LteNcRSRQ = (LteNcRSRQ - 40) / 2;
		if (GsmNcellCarrierRSSI >= 0)
			GsmNcellCarrierRSSI -= 101;
		if (TdsPccpchRSCP >= 0)
			TdsPccpchRSCP -= 116;

		if (LteScRSRP >= 0)
			LteScRSRP -= 141;
		if (LteScRSRQ >= 0)
			LteScRSRQ = (LteScRSRQ - 40) / 2;
		if (LteScSinrUL >= 0)
			LteScSinrUL -= 11;

		return true;
	}

}