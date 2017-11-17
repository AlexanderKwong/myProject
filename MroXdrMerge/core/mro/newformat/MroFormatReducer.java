package mro.newformat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class MroFormatReducer
{

	public static class StatReducer extends DataDealReducer<Text, Text, NullWritable, Text>
	{

		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		
		private String path_mroformat;
		private String[] strs;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
				
			//初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());	
			mosMng.SetOutputPath("mroformat", path_mroformat);
			
			mosMng.init();
			////////////////////
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

			String eutrancellId = "";
			boolean fillResult = true;

			StructData.SIGNAL_MR_All mrResult = new StructData.SIGNAL_MR_All();
			mrResult.Clear();

			for (Text value : values)
			{
				strs = value.toString().split("\\|", -1);

				StructData.MroOrigData item = new StructData.MroOrigData();

				try
				{
					fillResult = item.FillData(strs, 0);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "MroOrigData.FillData error ", e);
					continue;
				}

				if (!fillResult)
				{
					continue;
				}

				try
				{
					Date d_beginTime = format.parse(item.TimeStamp);
					mrResult.tsc.beginTime = (int) (d_beginTime.getTime() / 1000L);
				}
				catch (Exception e)
				{
					mrResult.tsc.beginTime = 0;
					continue;
				}

				mrResult.tsc.beginTimems = 0;
				mrResult.tsc.IMSI = 0;
				mrResult.tsc.TAC = 0;
				mrResult.tsc.ENBId = getValidValueInt(mrResult.tsc.ENBId, item.enbId);
				mrResult.tsc.UserLabel = getValidValueString(mrResult.tsc.UserLabel, item.userLabel);
				mrResult.tsc.Earfcn = getValidValueInt(mrResult.tsc.Earfcn, item.LteScEarfcn);
				mrResult.tsc.MmeCode = getValidValueInt(mrResult.tsc.MmeCode, item.MmeCode);
				mrResult.tsc.MmeGroupId = getValidValueInt(mrResult.tsc.MmeGroupId, item.MmeGroupId);
				mrResult.tsc.MmeUeS1apId = getValidValueLong(mrResult.tsc.MmeUeS1apId, item.MmeUeS1apId);
				mrResult.tsc.Weight = 0;
				mrResult.tsc.EventType = "";
				mrResult.tsc.LteScRSRP = getValidValueInt(mrResult.tsc.LteScRSRP, item.LteScRSRP);
				mrResult.tsc.LteScRSRQ = getValidValueInt(mrResult.tsc.LteScRSRQ, item.LteScRSRQ);
				mrResult.tsc.LteScEarfcn = getValidValueInt(mrResult.tsc.LteScEarfcn, item.LteScEarfcn);
				mrResult.tsc.LteScPci = getValidValueInt(mrResult.tsc.LteScPci, item.LteScPci);
				mrResult.tsc.LteScBSR = 0;
				mrResult.tsc.LteScRTTD = getValidValueInt(mrResult.tsc.LteScRTTD, item.LteScRTTD);
				mrResult.tsc.LteScTadv = 0;
				mrResult.tsc.LteScAOA = getValidValueInt(mrResult.tsc.LteScAOA, item.LteScAOA);
				mrResult.tsc.LteScPHR = getValidValueInt(mrResult.tsc.LteScPHR, item.LteScPHR);
				mrResult.tsc.LteScSinrUL = getValidValueInt(mrResult.tsc.LteScSinrUL, item.LteScSinrUL);
				mrResult.tsc.LteScRIP = getValidValueInt(mrResult.tsc.LteScRIP, item.LteScRIP);
				mrResult.tsc.LteScPlrULQci[0] = getValidValueInt(mrResult.tsc.LteScPlrULQci[0], item.LteScPlrULQci1);
				mrResult.tsc.LteScPlrULQci[1] = getValidValueInt(mrResult.tsc.LteScPlrULQci[1], item.LteScPlrULQci2);
				mrResult.tsc.LteScPlrULQci[2] = getValidValueInt(mrResult.tsc.LteScPlrULQci[2], item.LteScPlrULQci3);
				mrResult.tsc.LteScPlrULQci[3] = getValidValueInt(mrResult.tsc.LteScPlrULQci[3], item.LteScPlrULQci4);
				mrResult.tsc.LteScPlrULQci[4] = getValidValueInt(mrResult.tsc.LteScPlrULQci[4], item.LteScPlrULQci5);
				mrResult.tsc.LteScPlrULQci[5] = getValidValueInt(mrResult.tsc.LteScPlrULQci[5], item.LteScPlrULQci6);
				mrResult.tsc.LteScPlrULQci[6] = getValidValueInt(mrResult.tsc.LteScPlrULQci[6], item.LteScPlrULQci7);
				mrResult.tsc.LteScPlrULQci[7] = getValidValueInt(mrResult.tsc.LteScPlrULQci[7], item.LteScPlrULQci8);
				mrResult.tsc.LteScPlrULQci[8] = getValidValueInt(mrResult.tsc.LteScPlrULQci[8], item.LteScPlrULQci9);
				mrResult.tsc.LteScPlrDLQci[0] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[0], item.LteScPlrDLQci1);
				mrResult.tsc.LteScPlrDLQci[1] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[1], item.LteScPlrDLQci2);
				mrResult.tsc.LteScPlrDLQci[2] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[2], item.LteScPlrDLQci3);
				mrResult.tsc.LteScPlrDLQci[3] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[3], item.LteScPlrDLQci4);
				mrResult.tsc.LteScPlrDLQci[4] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[4], item.LteScPlrDLQci5);
				mrResult.tsc.LteScPlrDLQci[5] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[5], item.LteScPlrDLQci6);
				mrResult.tsc.LteScPlrDLQci[6] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[6], item.LteScPlrDLQci7);
				mrResult.tsc.LteScPlrDLQci[7] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[7], item.LteScPlrDLQci8);
				mrResult.tsc.LteScPlrDLQci[8] = getValidValueInt(mrResult.tsc.LteScPlrDLQci[8], item.LteScPlrDLQci9);

				mrResult.tsc.LteScRI1 = 0;
				mrResult.tsc.LteScRI2 = 0;
				mrResult.tsc.LteScRI4 = 0;
				mrResult.tsc.LteScRI8 = 0;
				mrResult.tsc.LteScPUSCHPRBNum = 0;
				mrResult.tsc.LteScPDSCHPRBNum = 0;
				mrResult.tsc.imeiTac = 0;

				eutrancellId = getValidValueString(eutrancellId, item.eutrancellId);
				mrResult.tsc.Eci = Long.parseLong(eutrancellId);

				statLteNbCell(mrResult, item);
				statGsmNbCell(mrResult, item);
				statTdsNbCell(mrResult, item);
			}
			
			if(mrResult.tsc.MmeUeS1apId <= 0)
			{
				return;
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

			int count = mrResult.tlte.length < ncLteList.size() ? mrResult.tlte.length : ncLteList.size();
			mrResult.nccount[0] = (byte) count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.tlte[i] = ncLteList.get(i).getValue();
			}

			// NC TDS
			List<Map.Entry<String, StructData.NC_TDS>> ncTdsList = new ArrayList<Map.Entry<String, StructData.NC_TDS>>(
					ncTdsMap.entrySet());
			Collections.sort(ncTdsList, new Comparator<Map.Entry<String, StructData.NC_TDS>>()
			{
				public int compare(Map.Entry<String, StructData.NC_TDS> o1, Map.Entry<String, StructData.NC_TDS> o2)
				{
					return o2.getValue().TdsPccpchRSCP - o1.getValue().TdsPccpchRSCP;
				}
			});

			count = mrResult.ttds.length < ncTdsList.size() ? mrResult.ttds.length : ncTdsList.size();
			mrResult.nccount[1] = (byte) count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.ttds[i] = ncTdsList.get(i).getValue();
			}

			// NC GSM
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
			mrResult.nccount[2] = (byte) count;

			for (int i = 0; i < count; ++i)
			{
				mrResult.tgsm[i] = ncGsmList.get(i).getValue();
			}

			
			// set cellid
			try
			{
				String[] strs = eutrancellId.split("-", -1);
				if (strs.length == 2)
				{
					mrResult.tsc.CellId = Long.parseLong(strs[0]) * 256 + Long.parseLong(strs[1]);
				}
			}
			catch (Exception e)
			{
				return;
			}

			// local positoin
			MrLoc.GetInstance().locLte(mrResult, mrResult.tsc.longitude, mrResult.tsc.latitude);

			curText.set(mrResult.GetDataEx());
			//curText.set(mrResult.GetData());
			mosMng.write("mroformat", NullWritable.get(), curText);
			
		}

		private void statLteNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigData item)
		{
			if (item.LteNcRSRP != StaticConfig.Int_Abnormal && item.LteNcEarfcn > 0 && item.LteNcPci > 0)
			{
				String key = item.LteNcEarfcn + "_" + item.LteNcPci;
				
				StructData.NC_LTE data = ncLteMap.get(key);
				if(data == null)
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

		private void statGsmNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigData item)
		{
			if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBsic > 0)
			{
				String key = item.GsmNcellBcch + "_" + item.GsmNcellBsic;
				
				StructData.NC_GSM data = ncGsmMap.get(key);
				if(data == null)
				{
					data = new StructData.NC_GSM();
					data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
					data.GsmNcellBsic = item.GsmNcellBsic;
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

		private void statTdsNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigData item)
		{
			if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0 && item.TdsCellParameterId > 0)
			{
				String key = item.TdsNcellUarfcn + "_" + item.TdsCellParameterId;
				
				StructData.NC_TDS data = ncTdsMap.get(key);
				if(data == null)
				{
					data = new StructData.NC_TDS();
					data.TdsPccpchRSCP = item.TdsPccpchRSCP;
					data.TdsNcellUarfcn = (byte) item.TdsNcellUarfcn;
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
