package mro.lablefillex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.IWriteLogCallBack.LogType;
import locuser.UserProp;
import locuser_v2.UserLocer;
import mro.lablefill.XdrLableMng;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.DataGeter;
import util.Func;

public class MroLocStat
{
	private static Map<String, StructData.NC_LTE> ncLteMap;
	private static Map<String, StructData.NC_GSM> ncGsmMap;
	private static Map<String, StructData.NC_TDS> ncTdsMap;
	protected static Logger LOG = Logger.getLogger(MroLocStat.class);

	public static Object getValidData(Object srcData, Object tarData)
	{
		if (tarData instanceof Integer)
		{
			if ((Integer) tarData != 0 && (Integer) tarData != StaticConfig.Int_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		else if (tarData instanceof Long)
		{
			if ((Long) tarData != 0 && (Long) tarData != StaticConfig.Long_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		return srcData;
	}

	private static void statLteNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
	{
		if (item.LteNcRSRP != StaticConfig.Int_Abnormal && item.LteNcEarfcn > 0 && item.LteNcPci > 0)
		{
			String key = item.LteNcEarfcn + "_" + item.LteNcPci;

			StructData.NC_LTE data = ncLteMap.get(key);
			if (data == null)
			{
				data = new StructData.NC_LTE();
				data.LteNcEarfcn = item.LteNcEarfcn;
				data.LteNcPci = item.LteNcPci;
				data.LteNcRSRP = item.LteNcRSRP;
				data.LteNcRSRQ = item.LteScRSRQ;
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
				{
					data.LteNcEnodeb = item.LteNcEnodeb;
					data.LteNcCid = item.LteNcCid;
				}

				ncLteMap.put(key, data);
			}
			else
			{
				if (item.LteNcRSRP > data.LteNcRSRP)
				{
					data.LteNcRSRP = item.LteNcRSRP;
					data.LteNcRSRQ = item.LteNcRSRQ;
				}
			}
		}
	}

	private static void statGsmNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
	{
		if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBcc > 0)
		{
			String key = item.GsmNcellBcch + "_" + item.GsmNcellBcc;

			StructData.NC_GSM data = ncGsmMap.get(key);
			if (data == null)
			{
				data = new StructData.NC_GSM();
				data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				data.GsmNcellBsic = item.GsmNcellNcc * 8 + item.GsmNcellBcc;
				data.GsmNcellBcch = item.GsmNcellBcch;
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
				{
					data.GsmNcellLac = item.GsmNcellLac;
					data.GsmNcellCi = item.GsmNcellCi;
				}

				ncGsmMap.put(key, data);
			}
			else
			{
				if (item.GsmNcellCarrierRSSI > data.GsmNcellCarrierRSSI)
				{
					data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
				}
			}
		}
	}

	private static void statTdsNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
	{
		if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0 && item.TdsCellParameterId > 0)
		{
			String key = item.TdsNcellUarfcn + "_" + item.TdsCellParameterId;

			StructData.NC_TDS data = ncTdsMap.get(key);
			if (data == null)
			{
				data = new StructData.NC_TDS();
				data.TdsPccpchRSCP = item.TdsPccpchRSCP;
				data.TdsNcellUarfcn = (short) item.TdsNcellUarfcn;
				data.TdsCellParameterId = (short) item.TdsCellParameterId;

				ncTdsMap.put(key, data);
			}
			else
			{
				if (item.TdsPccpchRSCP > data.TdsPccpchRSCP)
				{
					data.TdsPccpchRSCP = item.TdsPccpchRSCP;
				}
			}

		}
	}

	public static int getValidValueInt(int srcValue, int targValue)
	{
		if (targValue != StaticConfig.Int_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public static long getValidValueLong(long srcValue, long targValue)
	{
		if (targValue != StaticConfig.Long_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public static String getValidValueString(String srcValue, String targValue)
	{
		if (!targValue.equals(""))
		{
			return targValue;
		}
		return srcValue;
	}

	public static void mergeMro(HashMap<String, ArrayList<StructData.MroOrigDataMT>> map, Iterable<Text> values, DataAdapterReader dataAdapterReader, ParseItem parseItem)
	{
		try
		{
			map.clear();
			StringBuffer bfKey = new StringBuffer();
			int packet = 0;
			long eci = 0;

			for (Text mro : values)
			{
				packet++;
				if (packet % 1000 == 0)
					LOGHelper.GetLogger().writeLog(LogType.info, " mergeMro:" + eci + ", packet num=" + packet);
				if (packet > 50000)
				{
					LOG.info("mro num biger  than 50000 !");
					LOGHelper.GetLogger().writeLog(LogType.error, "mro num biger  than 50000 !");
					break;
				}
				bfKey.setLength(0);
				String[] valstrs = mro.toString().split(parseItem.getSplitMark(), -1);
				dataAdapterReader.readData(valstrs);
				StructData.MroOrigDataMT item = new StructData.MroOrigDataMT();
				if (!item.FillData(dataAdapterReader))
				{
					continue;
				}
				String tmStr = dataAdapterReader.GetStrValue("CellId", "0");
				int enbid = dataAdapterReader.GetIntValue("ENBId", -1);
				long cellid = 0L;
				if (tmStr.indexOf(":") > 0)
				{
					cellid = DataGeter.GetLong(tmStr.substring(0, tmStr.indexOf(":")));
				}
				else if (tmStr.indexOf("-") > 0)
				{
					cellid = DataGeter.GetLong(tmStr.substring(tmStr.indexOf("-") + 1));
				}
				else
				{
					cellid = DataGeter.GetLong(tmStr);
				}
				if (cellid < 256)
				{
					eci = enbid * 256 + cellid;
				}
				else
				{
					eci = cellid;
				}
				bfKey.append(eci);
				bfKey.append("_");
				bfKey.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
				bfKey.append("_");
				bfKey.append(dataAdapterReader.GetDateValue("beginTime", null));
				bfKey.append("_");
				bfKey.append(dataAdapterReader.GetStrValue("EventType", ""));
				String key = bfKey.toString();
				ArrayList<StructData.MroOrigDataMT> mroList = map.get(key);
				if (mroList == null)
				{
					mroList = new ArrayList<StructData.MroOrigDataMT>();
					map.put(key, mroList);
				}
				mroList.add(item);
			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "mergeMro error", e);
		}
	}

	public static void ottFix(SIGNAL_MR_All mroItem, XdrLableMng xdrLableMng, LteCellInfo cellInfo)
	{
		if (mroItem == null || mroItem.tsc == null || mroItem.tsc.MmeUeS1apId <= 0 || mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
			return;
		// 附上地市id
		mroItem.tsc.cityID = cellInfo.cityid;
		xdrLableMng.dealMroData(mroItem);// ott定位
	}

	/**
	 * ott定位format之后的数据
	 */
	public static List<SIGNAL_MR_All> formatedOttFixed(Iterable<Text> values, XdrLableMng xdrLableMng, LteCellInfo cellInfo)
	{
		Text value;
		String[] strs;
		List<SIGNAL_MR_All> allMroItemList = new ArrayList<SIGNAL_MR_All>();
		while (values.iterator().hasNext())
		{
			try
			{
				value = values.iterator().next();
				strs = value.toString().split(StaticConfig.DataSliper2, -1);
				SIGNAL_MR_All mroItem = new SIGNAL_MR_All();
				Integer i = 0;
				try
				{
					mroItem.FillData(new Object[] { strs, i });
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "SIGNAL_MR_All.FillData error ", e);
					continue;
				}
				ottFix(mroItem, xdrLableMng, cellInfo);
				allMroItemList.add(mroItem);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "mro collect err ");
			}
		}
		return allMroItemList;
	}

	/**
	 * 没有format的mro解码定位
	 */
	public static void srcMroOttFixed(List<SIGNAL_MR_All> allMdtItemList, SIGNAL_MR_All mroItem, XdrLableMng xdrLableMng, LteCellInfo cellInfo)
	{
		ottFix(mroItem, xdrLableMng, cellInfo);
		allMdtItemList.add(mroItem);
	}

	/**
	 * 指纹库算法1定位
	 */
	public static void UserLocFixed(UserProp userProp, List<SIGNAL_MR_All> allMroItemList, long eci)
	{
		if (userProp != null)
		{
			try
			{
				userProp.DoWork(eci, allMroItemList);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "userProp.DoWork error ", e);
			}
		}
	}

	/**
	 * 指纹库算法2定位
	 */
	public static void UserLoc2Fixed(UserLocer userLocer, List<SIGNAL_MR_All> allMroItemList, MultiOutputMng<NullWritable, Text> mosMng)
	{
		if (userLocer != null)
		{
			try
			{
				userLocer.DoWork(1, allMroItemList, mosMng);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "userLocer.DoWork error ", e);
			}
		}
	}

	public static StructData.SIGNAL_MR_All collectData(List<StructData.MroOrigDataMT> values)
	{
		ncLteMap = new HashMap<String, StructData.NC_LTE>();
		ncGsmMap = new HashMap<String, StructData.NC_GSM>();
		ncTdsMap = new HashMap<String, StructData.NC_TDS>();
		StructData.SIGNAL_MR_All mrResult = new StructData.SIGNAL_MR_All();
		mrResult.Clear();
		for (StructData.MroOrigDataMT item : values)
		{

			try
			{
				mrResult.tsc.beginTime = (int) (item.beginTime.getTime() / 1000L);
				mrResult.tsc.beginTimems = (int) (item.beginTime.getTime() % 1000L);
			}
			catch (Exception e)
			{
				mrResult.tsc.beginTime = 0;
				mrResult.tsc.beginTimems = 0;
				continue;
			}

			mrResult.tsc.IMSI = 0;
			mrResult.tsc.TAC = 0;
			mrResult.tsc.ENBId = getValidValueInt(mrResult.tsc.ENBId, item.ENBId);
			mrResult.tsc.UserLabel = getValidValueString(mrResult.tsc.UserLabel, item.UserLabel);
			mrResult.tsc.Earfcn = getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);
			mrResult.tsc.MmeCode = getValidValueInt(mrResult.tsc.MmeCode, item.MmeCode);
			mrResult.tsc.MmeGroupId = getValidValueInt(mrResult.tsc.MmeGroupId, item.MmeGroupId);
			mrResult.tsc.MmeUeS1apId = getValidValueLong(mrResult.tsc.MmeUeS1apId, item.MmeUeS1apId);
			mrResult.tsc.Weight = getValidValueInt(mrResult.tsc.Weight, item.Weight);
			mrResult.tsc.EventType = getValidValueString(mrResult.tsc.EventType, item.EventType);
			mrResult.tsc.LteScRSRP = getValidValueInt(mrResult.tsc.LteScRSRP, item.LteScRSRP);
			mrResult.tsc.LteScRSRQ = getValidValueInt(mrResult.tsc.LteScRSRQ, item.LteScRSRQ);
			mrResult.tsc.LteScEarfcn = getValidValueInt(mrResult.tsc.LteScEarfcn, item.LteScEarfcn);
			mrResult.tsc.LteScPci = getValidValueInt(mrResult.tsc.LteScPci, item.LteScPci);
			mrResult.tsc.LteScBSR = getValidValueInt(mrResult.tsc.LteScBSR, item.LteScBSR);
			mrResult.tsc.LteScRTTD = getValidValueInt(mrResult.tsc.LteScRTTD, item.LteScRTTD);
			mrResult.tsc.LteScTadv = getValidValueInt(mrResult.tsc.LteScTadv, item.LteScTadv);
			mrResult.tsc.LteScAOA = getValidValueInt(mrResult.tsc.LteScAOA, item.LteScAOA);
			mrResult.tsc.LteScPHR = getValidValueInt(mrResult.tsc.LteScPHR, item.LteScPHR);
			mrResult.tsc.LteScSinrUL = getValidValueInt(mrResult.tsc.LteScSinrUL, item.LteScSinrUL);
			mrResult.tsc.LteScRIP = getValidValueInt(mrResult.tsc.LteScRIP, item.LteScRIP);

			for (int i = 0; i < mrResult.tsc.LteScPlrULQci.length; ++i)
			{
				mrResult.tsc.LteScPlrULQci[i] = getValidValueInt(mrResult.tsc.LteScPlrULQci[i], item.LteScPlrULQci[i]);
			}

			for (int i = 0; i < mrResult.tsc.LteScPlrDLQci.length; ++i)
			{
				mrResult.tsc.LteScPlrDLQci[i] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[i], item.LteScPlrDLQci[i]);
			}

			mrResult.tsc.LteScRI1 = getValidValueInt(mrResult.tsc.LteScRI1, item.LteScRI1);
			mrResult.tsc.LteScRI2 = getValidValueInt(mrResult.tsc.LteScRI2, item.LteScRI2);
			mrResult.tsc.LteScRI4 = getValidValueInt(mrResult.tsc.LteScRI4, item.LteScRI4);
			mrResult.tsc.LteScRI8 = getValidValueInt(mrResult.tsc.LteScRI8, item.LteScRI8);
			mrResult.tsc.LteScPUSCHPRBNum = getValidValueInt(mrResult.tsc.LteScPUSCHPRBNum, item.LteScPUSCHPRBNum);
			mrResult.tsc.LteScPDSCHPRBNum = getValidValueInt(mrResult.tsc.LteScPDSCHPRBNum, item.LteScPDSCHPRBNum);
			mrResult.tsc.LteSceNBRxTxTimeDiff = getValidValueInt(mrResult.tsc.LteSceNBRxTxTimeDiff, item.LteSceNBRxTxTimeDiff);

			mrResult.tsc.Eci = item.ENBId * 256 + item.CellId;
			mrResult.tsc.CellId = mrResult.tsc.Eci;

			if (mrResult.tsc.MmeUeS1apId <= 0 || mrResult.tsc.Eci <= 0)
			{
				return null;
			}

			statLteNbCell(mrResult, item);
			statGsmNbCell(mrResult, item);
			statTdsNbCell(mrResult, item);

			// NC LTE
			List<StructData.NC_LTE> ncLteList = new ArrayList<StructData.NC_LTE>(ncLteMap.values());
			ncLteMap.clear();
			Collections.sort(ncLteList, new Comparator<StructData.NC_LTE>()
			{
				public int compare(StructData.NC_LTE o1, StructData.NC_LTE o2)
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
					if (cmccLteCount < mrResult.tlte.length)
					{
						mrResult.tlte[cmccLteCount] = ncItem;
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
			if (nclte_dx != null)
			{
				mrResult.LteScRSRP_DX = nclte_dx.LteNcRSRP;
				mrResult.LteScRSRQ_DX = nclte_dx.LteNcRSRQ;
				mrResult.LteScEarfcn_DX = nclte_dx.LteNcEarfcn;
				mrResult.LteScPci_DX = nclte_dx.LteNcPci;
			}
			if (nclte_lt != null)
			{
				mrResult.LteScRSRP_LT = nclte_lt.LteNcRSRP;
				mrResult.LteScRSRQ_LT = nclte_lt.LteNcRSRQ;
				mrResult.LteScEarfcn_LT = nclte_lt.LteNcEarfcn;
				mrResult.LteScPci_LT = nclte_lt.LteNcPci;
			}
			ncLteList.clear();

			// 添加联通数据
			if (nclte_lt != null && mrResult.fillNclte_Freq(nclte_lt))
			{
				lteCount_Freq++;
			}
			// 添加电信数据
			if (nclte_dx != null && mrResult.fillNclte_Freq(nclte_dx))
			{
				lteCount_Freq++;
			}

			mrResult.nccount[0] = (byte) cmccLteCount;
			mrResult.nccount[2] = (byte) (lteCount_Freq);

			// NC TDS
			// TD只保留前2个邻区
			List<StructData.NC_TDS> ncTdsList = new ArrayList<StructData.NC_TDS>(ncTdsMap.values());
			ncTdsMap.clear();
			Collections.sort(ncTdsList, new Comparator<StructData.NC_TDS>()
			{
				public int compare(StructData.NC_TDS o1, StructData.NC_TDS o2)
				{
					return o2.TdsPccpchRSCP - o1.TdsPccpchRSCP;
				}
			});

			int count = mrResult.ttds.length < ncTdsList.size() ? mrResult.ttds.length : ncTdsList.size();
			count = count > 2 ? 2 : count;
			mrResult.nccount[1] = (byte) count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.ttds[i] = ncTdsList.get(i);
			}
			ncTdsList.clear();

			// NC GSM
			// GSM只保留前1个邻区
			List<StructData.NC_GSM> ncGsmList = new ArrayList<StructData.NC_GSM>(ncGsmMap.values());
			ncGsmMap.clear();
			Collections.sort(ncGsmList, new Comparator<StructData.NC_GSM>()
			{
				public int compare(StructData.NC_GSM o1, StructData.NC_GSM o2)
				{
					return o2.GsmNcellCarrierRSSI - o1.GsmNcellCarrierRSSI;
				}
			});

			count = mrResult.tgsm.length < ncGsmList.size() ? mrResult.tgsm.length : ncGsmList.size();
			count = count > 1 ? 1 : count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.tgsm[i] = ncGsmList.get(i);
			}
			ncGsmList.clear();
		}
		return mrResult;
	}
}
