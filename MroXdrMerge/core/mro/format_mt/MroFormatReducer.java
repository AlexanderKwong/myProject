package mro.format_mt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.sun.org.apache.xalan.internal.xsltc.cmdline.Compile;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.Func;

public class MroFormatReducer
{

	public static class StatReducer extends DataDealReducer<Text, Text, NullWritable, Text>
	{

		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();

		private String path_mroformat;
		// private String path_tb_signal;
		private String[] strs;

		private ParseItem parseItem_MROSRC;

		private Map<String, StructData.NC_LTE> ncLteMap = new HashMap<String, StructData.NC_LTE>();
		private Map<String, StructData.NC_GSM> ncGsmMap = new HashMap<String, StructData.NC_GSM>();
		private Map<String, StructData.NC_TDS> ncTdsMap = new HashMap<String, StructData.NC_TDS>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);

			Configuration conf = context.getConfiguration();
			path_mroformat = conf.get("mastercom.mroxdrmerge.mroformat.path_mroformat");
			// path_tb_signal =
			// conf.get("mastercom.mroxdrmerge.mroformat.path_tb_signal");

			// 初始化输出控制
			// mosMng = new MultiOutputMng<NullWritable, Text>(context,
			// MainModel.GetInstance().getFsUrl());
			mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			mosMng.SetOutputPath("mroformat", path_mroformat);
			// mosMng.SetOutputPath("tbsignal", path_tb_signal);

			mosMng.init();
			////////////////////

			parseItem_MROSRC = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem_MROSRC == null)
			{
				throw new IOException("parse item do not get.");
			}
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);

			mosMng.close();
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{

			ncLteMap = new HashMap<String, StructData.NC_LTE>();
			ncGsmMap = new HashMap<String, StructData.NC_GSM>();
			ncTdsMap = new HashMap<String, StructData.NC_TDS>();

			boolean fillResult = true;

			StructData.SIGNAL_MR_All mrResult = new StructData.SIGNAL_MR_All();
			mrResult.Clear();

			DataAdapterReader dataAdapterReader_MROSRC = new DataAdapterReader(parseItem_MROSRC);
			for (Text value : values)
			{
				strs = value.toString().split(parseItem_MROSRC.getSplitMark(), -1);

				StructData.MroOrigDataMT item = new StructData.MroOrigDataMT();

				try
				{
					dataAdapterReader_MROSRC.readData(strs);
					fillResult = item.FillData(dataAdapterReader_MROSRC);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "StructData.MroOrigDataMT error ", e);
					continue;
				}

				if (!fillResult)
				{
					continue;
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
				mrResult.tsc.EventType = getValidValueString(mrResult.tsc.EventType, "MRO");
				mrResult.tsc.LteScRSRP = getValidValueInt(mrResult.tsc.LteScRSRP, item.LteScRSRP);
				if (mrResult.tsc.LteScRSRP == -141)
				{
					continue;
				}
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

				statLteNbCell(mrResult, item);
				statGsmNbCell(mrResult, item);
				statTdsNbCell(mrResult, item);
			}

			if (mrResult.tsc.MmeUeS1apId <= 0 || mrResult.tsc.Eci <= 0)
			{
				return;
			}

			// NC LTE
			List<Map.Entry<String, StructData.NC_LTE>> ncLteList = new ArrayList<Map.Entry<String, StructData.NC_LTE>>(ncLteMap.entrySet());
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
			List<Map.Entry<String, StructData.NC_TDS>> ncTdsList = new ArrayList<Map.Entry<String, StructData.NC_TDS>>(ncTdsMap.entrySet());
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
			List<Map.Entry<String, StructData.NC_GSM>> ncGsmList = new ArrayList<Map.Entry<String, StructData.NC_GSM>>(ncGsmMap.entrySet());
			Collections.sort(ncGsmList, new Comparator<Map.Entry<String, StructData.NC_GSM>>()
			{
				public int compare(Map.Entry<String, StructData.NC_GSM> o1, Map.Entry<String, StructData.NC_GSM> o2)
				{
					return o2.getValue().GsmNcellCarrierRSSI - o1.getValue().GsmNcellCarrierRSSI;
				}
			});

			// GSM只保留前1个邻区
			count = mrResult.tgsm.length < ncGsmList.size() ? mrResult.tgsm.length : ncGsmList.size();
			count = count > 1 ? 1 : count;
			// mrResult.nccount[2] = (byte) count;
			for (int i = 0; i < count; ++i)
			{
				mrResult.tgsm[i] = ncGsmList.get(i).getValue();
			}

			// neimeng 新增6组gsm
			int gsmCount = mrResult.tgsmall.length < ncGsmList.size() ? mrResult.tgsmall.length : ncGsmList.size();
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{
				for (int i = 0; i < gsmCount; ++i)
				{
					mrResult.tgsmall[i] = ncGsmList.get(i).getValue();
				}
			}

			/*
			 * LOGHelper.GetLogger().writeLog(LogType.info, "mrResult.tgsm[0] "
			 * +mrResult.tgsm[0] ); if(mrResult.tgsm[0]!= null) {
			 * curText.set(mrResult.GetDataEx()); mosMng.write("tbsignal",
			 * NullWritable.get(), curText);
			 * 
			 * }
			 */
			curText.set(mrResult.GetDataEx());
			// curText.set(mrResult.GetData());
			mosMng.write("mroformat", NullWritable.get(), curText);

		}

		private void statLteNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
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

		private void statGsmNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
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

		private void statTdsNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
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

		public int getValidValueInt(int srcValue, int targValue)
		{
			if (targValue != StaticConfig.Int_Abnormal)
			{
				return targValue;
			}
			return srcValue;
		}

		public long getValidValueLong(long srcValue, long targValue)
		{
			if (targValue != StaticConfig.Long_Abnormal)
			{
				return targValue;
			}
			return srcValue;
		}

		public String getValidValueString(String srcValue, String targValue)
		{
			if (!targValue.equals(""))
			{
				return targValue;
			}
			return srcValue;
		}

	}

}
