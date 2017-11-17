package mro.lablefill_mro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mro.lablefill.CellTimeKey;
import mro.lablefill.XdrLable;
import mro.lablefill.XdrLableMng;
import mroxdrmerge.MainModel;
import util.Func;
import util.ValidDataGeter;

public class MroLableFileReducer
{

	public static class MroDataFileReducers extends DataDealReducer<CellTimeKey, Text, NullWritable, Text>
	{
		protected static final Log LOG = LogFactory.getLog(MroDataFileReducers.class);

		// mr dealer
		private List<IMrDeal> mrDealerList;

		private Context context;
		private final int TimeSpan = 600;// 10分钟间隔
		private String[] strs;

		private XdrLableMng xdrLableMng;

		private Map<String, StructData.NC_LTE> ncLteMap = new HashMap<String, StructData.NC_LTE>();
		private Map<String, StructData.NC_GSM> ncGsmMap = new HashMap<String, StructData.NC_GSM>();
		private Map<String, StructData.NC_TDS> ncTdsMap = new HashMap<String, StructData.NC_TDS>();

		private ParseItem parseItem_MROSRC;
		private DataAdapterReader dataAdapterReader;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);

			// 初始化小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
				throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
			}

			////////////////////

			parseItem_MROSRC = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem_MROSRC == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem_MROSRC);

			////////////////////
			xdrLableMng = new XdrLableMng();

			// mr dealer
			mrDealerList = new ArrayList<IMrDeal>();

			// 打印状态日志
			LOGHelper.GetLogger().writeLog(LogType.info,
					"cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			try
			{
				outUserData();
				outAllData();
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
			}

			super.cleanup(context);

		}

		@Override
		public void reduce(CellTimeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{

			// xdr location 数据处理
			if (key.getDataType() == 1)
			{
				// xdrdata
				xdrLableMng = new XdrLableMng();

				for (Text value : values)
				{

					String[] strs = value.toString().split("\t", -1);

					for (int i = 0; i < strs.length; ++i)
					{
						strs[i] = strs[i].trim();
					}

					XdrLable xdrLable;
					try
					{
						xdrLable = XdrLable.FillData(strs, 0);
						xdrLableMng.addXdrLocItem(xdrLable);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "XdrLable.FillData error ", e);
						continue;
					}

				}
				xdrLableMng.init();
			}
			else
			{
				// mrodata values
				// 获取tmCellID+TimeStamp+MmeUeS1apId+EventType值，////////////////////////////////////////////////////////////////////////////////////////////////////////
				StructData.SIGNAL_MR_All mrResult = new StructData.SIGNAL_MR_All();

				mrResult.Clear();

				Map<String, List<StructData.MroOrigDataMT>> mrMap = new HashMap<String, List<StructData.MroOrigDataMT>>();
				String beginTime = "";
				String eci = "";
				String EventType = "";
				String MmeUeS1apId = "";
				for (Text value : values)
				{
					strs = value.toString().split(parseItem_MROSRC.getSplitMark(), -1);
					StructData.MroOrigDataMT item = new StructData.MroOrigDataMT();

					try
					{
						dataAdapterReader.readData(strs);
						item.FillData(dataAdapterReader);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "MroOrigDataMT.FillData error:  " + strs);
						continue;
					}

					beginTime = String.valueOf(item.beginTime.getTime() / 1000L);
					eci = String.valueOf((item.ENBId * 256 + item.CellId));
					EventType = item.EventType;
					MmeUeS1apId = String.valueOf(item.MmeUeS1apId);

					String keys = beginTime + eci + EventType + MmeUeS1apId;

					List<StructData.MroOrigDataMT> list = mrMap.get(keys);
					if (list == null)
					{
						list = new ArrayList<StructData.MroOrigDataMT>();
						mrMap.put(keys, list);
					}
					list.add(item);
				}

				Iterator<Entry<String, List<StructData.MroOrigDataMT>>> iterators = mrMap.entrySet().iterator();

				List<SIGNAL_MR_All> mroItemList = new ArrayList<SIGNAL_MR_All>();
				int curTimeSpan = 0;

				while (iterators.hasNext())
				{
					try
					{
						Entry<String, List<StructData.MroOrigDataMT>> next = iterators.next();
						List<StructData.MroOrigDataMT> value = next.getValue();
						SIGNAL_MR_All mroItem = collectData(value);

						if (mroItem == null || mroItem.tsc == null)
							continue;
						int time = mroItem.tsc.beginTime;

						if (curTimeSpan == 0)
						{
							curTimeSpan = mroItem.tsc.beginTime / TimeSpan * TimeSpan;
						}

						if (mroItem.tsc.MmeUeS1apId <= 0 || mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "mro collect err :  " + mrMap.size());
							continue;
						}

						// 附上地市id
						LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(mroItem.tsc.Eci);
						if (cellInfo != null)
						{
							mroItem.tsc.cityID = cellInfo.cityid;
						}

						xdrLableMng.dealMroData(mroItem);
						mroItemList.add(mroItem);

					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "mro collect err :  " + mrMap.size());
					}
				}
				outDealingData(mroItemList);
				mroItemList.clear();
			}

		}

		public StructData.SIGNAL_MR_All collectData(List<StructData.MroOrigDataMT> values)
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
				mrResult.tsc.ENBId = ValidDataGeter.getValidValueInt(mrResult.tsc.ENBId, item.ENBId);
				mrResult.tsc.UserLabel = ValidDataGeter.getValidValueString(mrResult.tsc.UserLabel, item.UserLabel);
				mrResult.tsc.Earfcn = ValidDataGeter.getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);
				mrResult.tsc.MmeCode = ValidDataGeter.getValidValueInt(mrResult.tsc.MmeCode, item.MmeCode);
				mrResult.tsc.MmeGroupId = ValidDataGeter.getValidValueInt(mrResult.tsc.MmeGroupId, item.MmeGroupId);
				mrResult.tsc.MmeUeS1apId = ValidDataGeter.getValidValueLong(mrResult.tsc.MmeUeS1apId, item.MmeUeS1apId);
				mrResult.tsc.Weight = ValidDataGeter.getValidValueInt(mrResult.tsc.Weight, item.Weight);
				mrResult.tsc.EventType = ValidDataGeter.getValidValueString(mrResult.tsc.EventType, item.EventType);
				mrResult.tsc.LteScRSRP = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRSRP, item.LteScRSRP);
				mrResult.tsc.LteScRSRQ = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRSRQ, item.LteScRSRQ);
				mrResult.tsc.LteScEarfcn = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScEarfcn, item.LteScEarfcn);
				mrResult.tsc.LteScPci = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScPci, item.LteScPci);
				mrResult.tsc.LteScBSR = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScBSR, item.LteScBSR);
				mrResult.tsc.LteScRTTD = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRTTD, item.LteScRTTD);
				mrResult.tsc.LteScTadv = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScTadv, item.LteScTadv);
				mrResult.tsc.LteScAOA = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScAOA, item.LteScAOA);
				mrResult.tsc.LteScPHR = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScPHR, item.LteScPHR);
				mrResult.tsc.LteScSinrUL = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScSinrUL, item.LteScSinrUL);
				mrResult.tsc.LteScRIP = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRIP, item.LteScRIP);

				for (int i = 0; i < mrResult.tsc.LteScPlrULQci.length; ++i)
				{
					mrResult.tsc.LteScPlrULQci[i] = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScPlrULQci[i],
							item.LteScPlrULQci[i]);
				}

				for (int i = 0; i < mrResult.tsc.LteScPlrDLQci.length; ++i)
				{
					mrResult.tsc.LteScPlrDLQci[i] = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScPlrDLQci[i],
							item.LteScPlrDLQci[i]);
				}

				mrResult.tsc.LteScRI1 = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRI1, item.LteScRI1);
				mrResult.tsc.LteScRI2 = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRI2, item.LteScRI2);
				mrResult.tsc.LteScRI4 = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRI4, item.LteScRI4);
				mrResult.tsc.LteScRI8 = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScRI8, item.LteScRI8);
				mrResult.tsc.LteScPUSCHPRBNum = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScPUSCHPRBNum,
						item.LteScPUSCHPRBNum);
				mrResult.tsc.LteScPDSCHPRBNum = ValidDataGeter.getValidValueInt(mrResult.tsc.LteScPDSCHPRBNum,
						item.LteScPDSCHPRBNum);
				mrResult.tsc.LteSceNBRxTxTimeDiff = ValidDataGeter.getValidValueInt(mrResult.tsc.LteSceNBRxTxTimeDiff,
						item.LteSceNBRxTxTimeDiff);

				mrResult.tsc.Eci = item.ENBId * 256 + item.CellId;
				mrResult.tsc.CellId = mrResult.tsc.Eci;

				statLteNbCell(mrResult, item);
				statGsmNbCell(mrResult, item);
				statTdsNbCell(mrResult, item);

				if (mrResult.tsc.MmeUeS1apId <= 0 || mrResult.tsc.Eci <= 0)
				{
					return null;
				}

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
				// mrResult.nccount[2] = (byte) count;

				for (int i = 0; i < count; ++i)
				{
					mrResult.tgsm[i] = ncGsmList.get(i).getValue();
				}
			}
			return mrResult;

		}

		// 吐出用户过程数据，为了防止内存过多
		private void outDealingData(List<SIGNAL_MR_All> mroItemList)
		{

		}

		// 将会吐出用户最后所有数据
		private void outUserData()
		{

		}

		private void outAllData()
		{

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

		private void statTdsNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
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
}
