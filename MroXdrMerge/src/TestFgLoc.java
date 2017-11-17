import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import locuser.UserProp;
import mro.lablefill.CellTimeKey;
import mroxdrmerge.MainModel;
import util.Func;

public class TestFgLoc
{
	public static void main(String[] args) throws Exception
	{	
		Configuration conf = new Configuration();
		String fsurl = "hdfs://" + MainModel.GetInstance().getAppConfig().getHadoopHost() + ":"
				+ MainModel.GetInstance().getAppConfig().getHadoopHdfsPort();
		conf.set("fs.defaultFS", fsurl);
		// 初始化小区的信息
		if (!CellConfig.GetInstance().loadLteCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
			throw (new IOException("cellconfig init error 请检查！"));
		}
		
	    HDFSOper hdfsOper = new HDFSOper(conf);
	    
	    BufferedReader reader = null;
	    ParseItem parseItem;
		String strData;
		String[] strs;	
	    ///读取多个厂家原始文件///////////////////////////////////////////////////////////
/*		
		DataAdapterReader dataAdapterReader;
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		dataAdapterReader = new DataAdapterReader(parseItem);
		Map<Long,Integer> mrMap1 = new HashMap<Long,Integer>();	    
	    for (int ii = 0; ii < 24; ii++)
	    {
	    	for (int jj = 0; jj < 10; jj++)
			{
	    		String filePath = "hdfs://192.168.1.31:9000/mt_wlyh/Data/mromt/170414/HUAWEI1_170414"+String.valueOf(ii)+"00_"+ String.valueOf(jj)+".MRO";
				if (ii < 10)
				{
					filePath = "hdfs://192.168.1.31:9000/mt_wlyh/Data/mromt/170414/HUAWEI1_1704140"+String.valueOf(ii)+"00_"+ String.valueOf(jj)+".MRO";
				}
	    		
				reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));

				while((strData = reader.readLine()) != null)
				{
					strs = strData.split(parseItem.getSplitMark(), -1);
					dataAdapterReader.readData(strs);
					
					StructData.SIGNAL_MR_All mrData = new StructData.SIGNAL_MR_All();
					try
					{
						mrData.FillData(dataAdapterReader);
						
						if (mrMap1.get(mrData.tsc.Eci) == null)
						{
							System.out.println(mrData.tsc.Eci + ",");
							mrMap1.put(mrData.tsc.Eci, 0);
						}
					}
					catch (Exception e)
					{
						continue;
					}
				}
				reader.close();
			}
	    }
*/	    
		////读取一个厂家原始文件/////////////////////////////////////////////////
/*    	String filePath = "hdfs://192.168.1.31:9000/mt_wlyh/Data/mromt/170414/HUAWEI1_1704141600_5.MRO";
		reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
		
		DataAdapterReader dataAdapterReader;
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		dataAdapterReader = new DataAdapterReader(parseItem);
		Map<Long,List<SIGNAL_MR_All>> mrMap = new HashMap<Long,List<SIGNAL_MR_All>>();
		
		while((strData = reader.readLine()) != null)
		{
			strs = strData.split(parseItem.getSplitMark(), -1);
			dataAdapterReader.readData(strs);
			
			StructData.SIGNAL_MR_All mrData = new StructData.SIGNAL_MR_All();
			try
			{
				mrData.FillData(dataAdapterReader);
			}
			catch (Exception e)
			{
				continue;
			}
			
		
			List<SIGNAL_MR_All> mrList = mrMap.get(mrData.tsc.Eci);
            if(mrList == null)
            {
            	//System.out.println(mrData.tsc.Eci + ",");
            	mrList = new ArrayList<SIGNAL_MR_All>();
            	mrMap.put(mrData.tsc.Eci, mrList);
            }
            mrList.add(mrData);
            
			//if(mrList.size() > 100)
			//{
			//	Collections.sort(mrList,new Comparator<SIGNAL_MR_All>(){  
			//        @Override  
			//        public int compare(SIGNAL_MR_All a, SIGNAL_MR_All b) {  
			//            return a.tsc.beginTime - b.tsc.beginTime;  
			//        }           
			//    });
			//	
			//	long eci = mrList.get(0).tsc.Eci;
			//    UserProp userProp = new UserProp();
			//	userProp.DoWork(eci, mrList);
			//	
			//	mrList.clear();
			//}

		}
		*////读取format后的文件/////////////////////////////////////////////////
	   	String filePath = "hdfs://192.168.1.31:9000/mt_wlyh/Data/mromt/170414/HUAWEI1_1704140000_0.MRO";
		reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
		
		DataAdapterReader dataAdapterReader;
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		dataAdapterReader = new DataAdapterReader(parseItem);
		Map<Long,List<SIGNAL_MR_All>> mrMap = new HashMap<Long,List<SIGNAL_MR_All>>();
		StructData.SIGNAL_MR_All mrResult = new StructData.SIGNAL_MR_All();
		HashMap<String, StructData.NC_LTE> ncLteMap = new HashMap<String, StructData.NC_LTE>();
		HashMap<String, StructData.NC_GSM> ncGsmMap = new HashMap<String, StructData.NC_GSM>();
		HashMap<String, StructData.NC_TDS> ncTdsMap = new HashMap<String, StructData.NC_TDS>();		
		mrResult.Clear();
		while((strData = reader.readLine()) != null)
		{
			strs = strData.split(parseItem.getSplitMark(), -1);
			dataAdapterReader.readData(strs);
			StructData.MroOrigDataMT item = new StructData.MroOrigDataMT();
			try
			{
				item.FillData(dataAdapterReader);
			}
			catch (Exception e)
			{
				continue;
			}
			
			int Weight = getValidValueInt(mrResult.tsc.Weight, item.Weight);
			
			if (Weight == 1 && mrResult.tsc.MmeUeS1apId > 0)
			{
				 OutputOneMr(ncLteMap, ncGsmMap, ncTdsMap, mrResult, mrMap);
				 mrResult = new StructData.SIGNAL_MR_All();
				 mrResult.Clear();
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
		
		/////////////////////////////////////////////////////		
		reader.close();
		for (List<SIGNAL_MR_All> mm : mrMap.values())
		{
        	long eci = mm.get(0).tsc.Eci;
        	
			Collections.sort(mm,new Comparator<SIGNAL_MR_All>(){  
				@Override  
				public int compare(SIGNAL_MR_All a, SIGNAL_MR_All b) {  
				    return a.tsc.beginTime - b.tsc.beginTime;  
				}           
			});        	
        	
        	if (eci == 8442881)
			{
        		UserProp userProp = new UserProp();
        		userProp.DoWork(eci, mm);
        		
//        		for (SIGNAL_MR_All aa : mm)
//        		{
//        			System.out.println(aa.tsc.beginTime + ","+aa.tsc.beginTimems+","+aa.ispeed);
//        		}
        	}
		}		
		
	}

	private static void OutputOneMr(HashMap<String, StructData.NC_LTE> ncLteMap,
			HashMap<String, StructData.NC_GSM> ncGsmMap, HashMap<String, StructData.NC_TDS> ncTdsMap,
			StructData.SIGNAL_MR_All mrResult, Map<Long,List<SIGNAL_MR_All>> mrMap)
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
	
			if (Func.checkFreqIsLtDx(ncItem.LteNcEarfcn))
			{
				//联通只保留最强小区
				if(ncItem.LteNcEarfcn == 1600
			     || ncItem.LteNcEarfcn == 1650
			     || ncItem.LteNcEarfcn == 40340)
				{
					
					if(nclte_lt == null || ncItem.LteNcRSRP > nclte_lt.LteNcRSRP)
					{
						nclte_lt = ncItem;
					}
				}
				//电信只保留最强小区
				else if(ncItem.LteNcEarfcn == 1775
					     || ncItem.LteNcEarfcn == 1800
					     || ncItem.LteNcEarfcn == 1825
					     || ncItem.LteNcEarfcn == 1850
					     || ncItem.LteNcEarfcn == 75
					     || ncItem.LteNcEarfcn == 100)
				{
					if(nclte_dx == null || ncItem.LteNcRSRP > nclte_dx.LteNcRSRP)
					{
						nclte_dx = ncItem;
					}
				}
			}
			else
			{
				if (cmccLteCount < mrResult.tlte.length)
				{
					mrResult.tlte[cmccLteCount] = ncItem;
					cmccLteCount++;
				}
			}
	
	
		}
		
		//添加联通数据
		if(nclte_lt != null && mrResult.fillNclte_Freq(nclte_lt))
		{
			lteCount_Freq++;
		}
		//添加电信数据
		if(nclte_dx != null && mrResult.fillNclte_Freq(nclte_dx))
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
	
		if (mrResult.tsc.Eci == 8442881 && mrResult.tsc.MmeUeS1apId == 201416966)
		{	
			List<SIGNAL_MR_All> mrList = mrMap.get(mrResult.tsc.Eci);
			if(mrList == null)
			{
			  	//System.out.println(mrData.tsc.Eci + ",");
			  	mrList = new ArrayList<SIGNAL_MR_All>();
			  	mrMap.put(mrResult.tsc.Eci, mrList);
			}
			mrList.add(mrResult);
		}
		mrResult = null;
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
