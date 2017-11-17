import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.math3.analysis.function.Sigmoid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import com.chinamobile.xdr.LocationInfo;
import com.chinamobile.xdr.demo;
import com.sun.corba.se.spi.orb.StringPair;

import StructData.DT_Sample_4G;

import StructData.SIGNAL_MR_All;

import StructData.SIGNAL_XDR_4G;
import cellconfig.CellConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.CompileCfgMng;
import jan.util.CompileList;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.MrLocation;
import util.MrLocationItem;
import xdr.lablefill.LocationItem;
import xdr.locallex.EventData;
import xdr.locallex.LocItem;
public class TestFunc
{

	public static void main(String[] args) throws Exception
	{
		try
		{
			for(int i=0; i<10000; ++i)
			{
//				XdrDataBase item = XdrDataFactory.GetInstance().getXdrDataObject(XdrDataFactory.XdrDataType_MOS_BEIJING);
				
			}
		}
		catch (Exception e)
		{
			int a = 1;
			a = 2;
		}


		
		
		
//		CellConfig.GetInstance().loadLteCell(new Configuration());
		
		//testMr();
		testFillData();
//		testSEQ();
//		testMr();
//		
//		
//		//MrLocationItem item = MrLocation.calcLongLac();
//		
//		if(MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
//		{
//		   int i = 0;
//		   i = 1;
//		}
//		
//		
//		MainModel.GetInstance().loadConfig();
//		
//		MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi);
//		
//		//testCombileConfig();
//		//testHxXdr();
//		//getImsi();
//		//testSEQ();
//		
//		testFileSize();
		
		
	}
	
	public static void testCombileConfig()
	{
		CompileList cl = new CompileList();
		//CompileCfgMng.getInstance().saveXml("C:\\Users\\Jancan\\Desktop\\compile.xml", cl);
	}
	
	public static void testFileSize()
	{
		try
		{
		    HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
	                MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
		    
		    long size = hdfsOper.getSizeOfPath("hdfs://192.168.1.65:9000/mt_wlyh/Data/mroxdrmerge/xdr_loc/data_01_160521/XDR_LOCATION_01_160521", false);
		}
		catch (Exception e)
		{
			// TODO: handle exception
		}

	    
	}
	
	public static void saveConfig()
	{
		try
		{
			 MainModel.GetInstance();
			 System.out.println(MainModel.GetInstance().getAppConfig().getHadoopHost());
			 MainModel.GetInstance().getAppConfig().saveConfigure();
		}
		catch (Exception e)
		{
			// TODO: handle exception
		}

	}
	
	public static void mergeFile()
	{		
		try
		{
			 HDFSOper oper = new HDFSOper("192.168.1.65", 9000);
			 oper.mergeDirSmallFiles("hdfs://192.168.1.65:9000/mt_wlyh/Data/test",
			 "\n", 30*1024*1024);	
		}
		catch (Exception e)
		{
			// TODO: handle exception
		}

	}

	public static void getImsi()
	{
		String imsi = "460002152712167";
		int count = Math.abs((imsi).hashCode()) % 50;
	}
	
	
	
	public static void testMr() throws IOException
	{
		try
		{
			{
				
				DataAdapterReader dataAdapterReader;
				int splitMax = -1;
				
				ParseItem parseItem_MROSRC = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-UE");
				if (parseItem_MROSRC == null)
				{
					throw new IOException("parse item do not get.");
				}
				dataAdapterReader = new DataAdapterReader(parseItem_MROSRC);
				String data = "1476324549,118,10302,3,000000000000000026c5b34f57feec8f,6,585EA65A04342896D783A4A8272EFB3E,35573607,CB88E66284E9975429AC57E7C6121511,,44102538,,,179143,45860610,460,1476324495070,1,,,,,,38400,42,6,1,459,38400,33,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,860755,2016-10-13 10:08:15,20161013,10,SZ,UE_MR,01713802558802001476324495070682722,SZ,2016101310";
				
				
				String[] strs = data.toString().split(parseItem_MROSRC.getSplitMark(), -1);

				SIGNAL_MR_All item = new SIGNAL_MR_All();
				dataAdapterReader.readData(strs);
				item.FillData(dataAdapterReader);	
				
			}
			
			
			{
				DataAdapterReader dataAdapterReader;
				int splitMax = -1;
				
				ParseItem parseItem_MROSRC = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
				if (parseItem_MROSRC == null)
				{
					throw new IOException("parse item do not get.");
				}
				dataAdapterReader = new DataAdapterReader(parseItem_MROSRC);
				String data = "	5	170580ddc6786b00	6	460027245539406	8613650309	13555312460	2	2016-10-13 15:11:37.234000	2016-10-13 15:11:37.280000	0	306240021			EE0D508A	577	196	EE01508A		100.72.255.129	100.72.196.152	17929	79144755			CMNET		";
				
				
				String[] strs = data.toString().split(parseItem_MROSRC.getSplitMark(), -1);

				SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
				dataAdapterReader.readData(strs);
				
				String beginTime = dataAdapterReader.GetStrValue("Procedure_Start_Time", "");
				String imsi = dataAdapterReader.GetStrValue("IMSI", "");
			
				if (beginTime.length() == 0 || imsi.trim().length() != 15)
				{
					//LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
					return;
				}
				
				if (!xdrItem.FillData_SEQ_MME(dataAdapterReader))
				{
					return;
				}
					
			}
		    
		}
		catch (Exception e)
		{
			int a = 0;
			a = 1;
		}

	}

	public static void testSEQ() 
	{

		try
		{
			MainModel.GetInstance().loadConfig();
			
			ParseItem parseItem_MME = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
			if(parseItem_MME == null)
			{
				throw new IOException("parse item do not get.");
			}
			DataAdapterReader dataAdapterReader_MME = new DataAdapterReader(parseItem_MME);
			
			ParseItem parseItem_HTTP = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
			if(parseItem_HTTP == null)
			{
				throw new IOException("parse item do not get.");
			}
			DataAdapterReader dataAdapterReader_HTTP = new DataAdapterReader(parseItem_HTTP);
			
			ParseItem parseItem_HX = MainModel.GetInstance().getDataAdapterConfig().getParseItem("HX-XDR");
			if(parseItem_HX == null)
			{
				throw new IOException("parse item do not get.");
			}
			DataAdapterReader dataAdapterReader_HX = new DataAdapterReader(parseItem_HX);
			
			
//			String data = "021|460036295706671|8693100263015500|8610936221788|6320|25297154|cmnet.mnc002.mcc460.gprs|103|1478314798540|1478314798756|15|21||0|0|117.144.106.180||1203|1687|vs1.tjcm.u3.ucweb.com|http://vs1.tjcm.u3.ucweb.com/?ucid=1609-5212448879-d6cb4e7f";
//					
//			String[] vals = data.toString().split(parseItem_HTTP.getSplitMark(), -1);
//
//			SIGNAL_XDR_4G item = new SIGNAL_XDR_4G();
//			dataAdapterReader_HTTP.readData(vals);
//			item.FillData_SEQ_HTTP(dataAdapterReader_HTTP);
			
			
			
			String data = "021|460037389024158|8698970249385300|8611074574200|1|1478359915380|1478359915619|0|6181|26889997|6181||||538437585|609|206";
			
	        String[] vals = data.toString().split(parseItem_HTTP.getSplitMark(), -1);

	        SIGNAL_XDR_4G item = new SIGNAL_XDR_4G();
	        dataAdapterReader_MME.readData(vals);
	        item.FillData_SEQ_MME(dataAdapterReader_MME);		
			
			
			
			
			
			
			
			
			
			
			
			// read file content from file
			StringBuffer sb = new StringBuffer("");

			FileReader reader = new FileReader("C://Users//Jancan//Desktop//http_test//http_460000606018293");
			BufferedReader br = new BufferedReader(reader);

			String str = null;

			while ((str = br.readLine()) != null)
			{
				String[] strs = str.toString().split(parseItem_MME.getSplitMark(), 53);

				SIGNAL_XDR_4G xdrItem = null;

				// new 4g xdr data
				if (strs.length == 26 || strs.length == 53)
				{
					xdrItem = new SIGNAL_XDR_4G();
					// xdr mme
					if (strs.length == 26)
					{
						dataAdapterReader_MME.readData(strs);
						if (!xdrItem.FillData_SEQ_MME(dataAdapterReader_MME))
						{
							return;
						}
					}
					// xdr http
					else if (strs.length == 53)
					{
						dataAdapterReader_HTTP.readData(strs);
						if (!xdrItem.FillData_SEQ_HTTP(dataAdapterReader_HTTP))
						{
							return;
						}
					}
				}
			}

			br.close();
			reader.close();

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}
	public static void testFillData()
	{
//		ParseItem parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("ERR-MOS");
//		DataAdapterReader curDataAdapterReader = new DataAdapterReader(parseItem);
//		
//		String value ="2017-07-20 10:56:56,30989,SXZC,31117,1,2,0,2,0,0.33,0.00,1045548739056245047,35543807,460007915C8F,460007915C8F,54,2409:8800:8242:4997:1c8c:a25d:9dca:3d2f,2409:8010:8210:1:1004:1004:0:1,49120,57552,AMR-WB,23.85,23.85,2.6438,232,7,852,129,0.1514,4.3256,232,7,1015,1,0.001,2.6593,232,0,854,129,0.1511,4.3293,0,1031,1,0.001,0,N,N,N,N,N,N,5025690,268912505,5025690,268912505";
//		String[] strs = value.toString().split(parseItem.getSplitMark(), -1);
//		curDataAdapterReader.readData(strs);
//		
////		XdrData_MOS_BeiJing sampleItem = null ; // new XdrData_MOS_BeiJing();
//		try
//		{
//			sampleItem.FillData(curDataAdapterReader);
//			ArrayList<EventData> eventDataList = new ArrayList<EventData>();
//			eventDataList = sampleItem.toEventData();
//			StringBuffer sb = new StringBuffer();
//			for(EventData eventData : eventDataList)
//			{
//				eventData.toString(sb);
//				String aa = sb.toString();
//				int aabb = 1;
//			}
//			
//		}
//		catch (Exception e)
//		{
//			LOGHelper.GetLogger().writeLog(LogType.error, "DT_Sample_4G.FillData error ", e);
//			//continue;
//		}
	}
}
