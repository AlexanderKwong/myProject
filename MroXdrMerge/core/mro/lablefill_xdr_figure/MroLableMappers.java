package mro.lablefill_xdr_figure;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.IWriteLogCallBack.LogType;
import mro.lablefill.CellTimeKey;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.Func;

public class MroLableMappers
{
	public static class FigureMapper extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci;
		private int time = 0;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split("\\|", -1);
			CellTimeKey itemKey;

			if (valstrs.length == 0)
			{
				return;
			}

			for (int i = 0; i < valstrs.length; i++)
			{
				String values[] = valstrs[i].split(",");
				if (values.length < 2)
				{
					return;
				}
				if (i == 0)
				{
					eci = Long.parseLong(values[1]);
				}
				else
				{
					eci = Long.parseLong(values[0]);
				}
				itemKey = new CellTimeKey(eci, time, 0);
				context.write(itemKey, value);
			}
		}
	}

	public static class XdrLocationMappers extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			final Log LOG = LogFactory.getLog(XdrLocationMappers.class);
			String[] valstrs = value.toString().split("\t", -1);

			if (valstrs.length < 11)
			{
				return;
			}

			eci = Long.parseLong(valstrs[1].trim());
			time = Integer.parseInt(valstrs[3].trim());

			int tmTime = time / TimeSpan * TimeSpan;
			CellTimeKey keyItem = new CellTimeKey(eci, tmTime - TimeSpan, 1);
			context.write(keyItem, value);

			keyItem = new CellTimeKey(eci, tmTime, 1);
			context.write(keyItem, value);

			keyItem = new CellTimeKey(eci, tmTime + TimeSpan, 1);
			context.write(keyItem, value);
		}

	}

	public static class MroDataMappers extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private String eci = "";
		private String beginTime = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private int splitMax = -1;
		private ParseItem parseItem;
		private Text resultValue = new Text();
		private DataAdapterReader dataAdapterReader_MROSRC;
		private StructData.SIGNAL_MR_All mrResult;
		private HashMap<String, StructData.NC_LTE> ncLteMap;
		private HashMap<String, StructData.NC_GSM> ncGsmMap;
		private HashMap<String, StructData.NC_TDS> ncTdsMap;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			ncLteMap = new HashMap<String, StructData.NC_LTE>();
			ncGsmMap = new HashMap<String, StructData.NC_GSM>();
			ncTdsMap = new HashMap<String, StructData.NC_TDS>();

			mrResult = new StructData.SIGNAL_MR_All();
			mrResult.Clear();
			dataAdapterReader_MROSRC = new DataAdapterReader(parseItem);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			if (mrResult.tsc.MmeUeS1apId > 0)
			{
				OutputOneMr(ncLteMap, ncGsmMap, ncTdsMap, mrResult, context);
			}
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{

			boolean fillResult = true;
			String[] strs = null;
			strs = value.toString().split(parseItem.getSplitMark(), -1);
			StructData.MroOrigDataMT item = new StructData.MroOrigDataMT();
			try
			{
				dataAdapterReader_MROSRC.readData(strs);
				fillResult = item.FillData(dataAdapterReader_MROSRC);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "StructData.MroOrigDataMT error ", e);
				// continue;
			}
			if (!fillResult)
			{
				// continue;
			}
			int Weight = getValidValueInt(mrResult.tsc.Weight, item.Weight);

			if (Weight == 1 && mrResult.tsc.MmeUeS1apId > 0)
			{
				OutputOneMr(ncLteMap, ncGsmMap, ncTdsMap, mrResult, context);
			}

			try
			{
				mrResult.tsc.beginTime = (int) (item.beginTime.getTime() / 1000L);
				mrResult.tsc.beginTimems = (int) (item.beginTime.getTime() % 1000L);
			}
			catch (Exception e)
			{
				mrResult.tsc.beginTime = 0;
				mrResult.tsc.beginTimems = 0;
				// continue;
			}

			mrResult.tsc.Weight = Weight;
			mrResult.tsc.IMSI = 0;
			mrResult.tsc.TAC = 0;
			mrResult.tsc.ENBId = getValidValueInt(mrResult.tsc.ENBId, item.ENBId);
			// mrResult.tsc.UserLabel =
			// getValidValueString(mrResult.tsc.UserLabel, item.UserLabel);
			mrResult.tsc.Earfcn = getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);
			// mrResult.tsc.MmeCode = getValidValueInt(mrResult.tsc.MmeCode,
			// item.MmeCode);
			// mrResult.tsc.MmeGroupId =
			// getValidValueInt(mrResult.tsc.MmeGroupId, item.MmeGroupId);
			if (Weight == 1 && mrResult.tsc.MmeUeS1apId <= 0)
			{
				mrResult.tsc.MmeUeS1apId = getValidValueLong(mrResult.tsc.MmeUeS1apId, item.MmeUeS1apId);
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
					mrResult.tsc.LteScPlrULQci[i] = getValidValueInt(mrResult.tsc.LteScPlrULQci[i],
							item.LteScPlrULQci[i]);
				}

				for (int i = 0; i < mrResult.tsc.LteScPlrDLQci.length; ++i)
				{
					mrResult.tsc.LteScPlrDLQci[i] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[i],
							item.LteScPlrDLQci[i]);
				}

				mrResult.tsc.LteScRI1 = getValidValueInt(mrResult.tsc.LteScRI1, item.LteScRI1);
				mrResult.tsc.LteScRI2 = getValidValueInt(mrResult.tsc.LteScRI2, item.LteScRI2);
				mrResult.tsc.LteScRI4 = getValidValueInt(mrResult.tsc.LteScRI4, item.LteScRI4);
				mrResult.tsc.LteScRI8 = getValidValueInt(mrResult.tsc.LteScRI8, item.LteScRI8);
				mrResult.tsc.LteScPUSCHPRBNum = getValidValueInt(mrResult.tsc.LteScPUSCHPRBNum, item.LteScPUSCHPRBNum);
				mrResult.tsc.LteScPDSCHPRBNum = getValidValueInt(mrResult.tsc.LteScPDSCHPRBNum, item.LteScPDSCHPRBNum);
				mrResult.tsc.LteSceNBRxTxTimeDiff = getValidValueInt(mrResult.tsc.LteSceNBRxTxTimeDiff,
						item.LteSceNBRxTxTimeDiff);

				mrResult.tsc.Eci = item.ENBId * 256 + item.CellId;
				mrResult.tsc.CellId = mrResult.tsc.Eci;
			}
			statLteNbCell(mrResult, item, ncLteMap);
			statGsmNbCell(mrResult, item, ncGsmMap);
			statTdsNbCell(mrResult, item, ncTdsMap);
		}

		private void OutputOneMr(HashMap<String, StructData.NC_LTE> ncLteMap,
				HashMap<String, StructData.NC_GSM> ncGsmMap, HashMap<String, StructData.NC_TDS> ncTdsMap,
				StructData.SIGNAL_MR_All mrResult, Context context)
		{
			// NC LTE
			List<Map.Entry<String, StructData.NC_LTE>> ncLteList = new ArrayList<Map.Entry<String, StructData.NC_LTE>>(
					ncLteMap.entrySet());
			Collections.sort(ncLteList, new Comparator<Map.Entry<String, StructData.NC_LTE>>()
			{
				public int compare(Map.Entry<String, StructData.NC_LTE> o1, Map.Entry<String, StructData.NC_LTE> o2)
				{
					return o2.getValue().LteNcRSRP - o1.getValue().LteNcRSRP;
				}
			});

			int cmccLteCount = 0;
			int lteCount_Freq = 0;

			StructData.NC_LTE nclte_lt = null;
			StructData.NC_LTE nclte_dx = null;

			for (int i = 0; i < ncLteList.size(); ++i)
			{
				StructData.NC_LTE ncItem = ncLteList.get(i).getValue();

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
			List<Map.Entry<String, StructData.NC_TDS>> ncTdsList = new ArrayList<Map.Entry<String, StructData.NC_TDS>>(
					ncTdsMap.entrySet());
			Collections.sort(ncTdsList, new Comparator<Map.Entry<String, StructData.NC_TDS>>()
			{
				public int compare(Map.Entry<String, StructData.NC_TDS> o1, Map.Entry<String, StructData.NC_TDS> o2)
				{
					return o2.getValue().TdsPccpchRSCP - o1.getValue().TdsPccpchRSCP;
				}
			});

			int count = mrResult.ttds.length < ncTdsList.size() ? mrResult.ttds.length : ncTdsList.size();
			count = count > 2 ? 2 : count;
			mrResult.nccount[1] = (byte) count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.ttds[i] = ncTdsList.get(i).getValue();
			}

			// NC GSM
			// GSM只保留前1个邻区
			List<Map.Entry<String, StructData.NC_GSM>> ncGsmList = new ArrayList<Map.Entry<String, StructData.NC_GSM>>(
					ncGsmMap.entrySet());
			Collections.sort(ncGsmList, new Comparator<Map.Entry<String, StructData.NC_GSM>>()
			{
				public int compare(Map.Entry<String, StructData.NC_GSM> o1, Map.Entry<String, StructData.NC_GSM> o2)
				{
					return o2.getValue().GsmNcellCarrierRSSI - o1.getValue().GsmNcellCarrierRSSI;
				}
			});

			count = mrResult.tgsm.length < ncGsmList.size() ? mrResult.tgsm.length : ncGsmList.size();
			count = count > 1 ? 1 : count;
			mrResult.nccount[2] = (byte) count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.tgsm[i] = ncGsmList.get(i).getValue();
			}

			String dataEx = mrResult.GetDataEx();
			resultValue.set(dataEx);

			CellTimeKey keyItem = new CellTimeKey(mrResult.tsc.Eci, mrResult.tsc.beginTime / TimeSpan * TimeSpan, 2);
			try
			{
				context.write(keyItem, resultValue);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
			}

			mrResult.Clear();
			ncLteMap.clear();
			ncGsmMap.clear();
			ncTdsMap.clear();
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

		private static void statLteNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item,
				HashMap<String, StructData.NC_LTE> ncLteMap)
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

		private static void statGsmNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item,
				HashMap<String, StructData.NC_GSM> ncGsmMap)
		{
			if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBcc > 0)
			{
				String key = item.GsmNcellBcch + "_" + item.GsmNcellBcc;

				StructData.NC_GSM data = ncGsmMap.get(key);
				if (data == null)
				{
					data = new StructData.NC_GSM();
					data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
					data.GsmNcellBsic = item.GsmNcellBcc;
					data.GsmNcellBcch = item.GsmNcellBcch;

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

		private static void statTdsNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item,
				HashMap<String, StructData.NC_TDS> ncTdsMap)
		{
			if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0
					&& item.TdsCellParameterId > 0)
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

	}

	public static class CellPartitioner extends Partitioner<CellTimeKey, Text> implements Configurable
	{
		private Configuration conf = null;

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public void setConf(Configuration conf)
		{
			this.conf = conf;
		}

		@Override
		public int getPartition(CellTimeKey key, Text value, int numOfReducer)
		{
			return Math.abs(String.valueOf(key.getEci()).hashCode()) % numOfReducer;
		}
	}

	public static class CellSortKeyComparator extends WritableComparator
	{
		public CellSortKeyComparator()
		{
			super(CellTimeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			CellTimeKey s1 = (CellTimeKey) a;
			CellTimeKey s2 = (CellTimeKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class CellSortKeyGroupComparator extends WritableComparator
	{

		public CellSortKeyGroupComparator()
		{
			super(CellTimeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			CellTimeKey s1 = (CellTimeKey) a;
			CellTimeKey s2 = (CellTimeKey) b;

			if (s1.getEci() > s2.getEci())
			{
				return 1;
			}
			else if (s1.getEci() < s2.getEci())
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}
	}

}
