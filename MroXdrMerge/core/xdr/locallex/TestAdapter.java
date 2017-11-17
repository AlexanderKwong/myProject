package xdr.locallex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;



import StructData.StaticConfig;
import jan.util.DataAdapterReader;
import jan.util.DataAdapterConf.ParseItem;
import xdr.locallex.model.XdrDataBase;
import xdr.locallex.model.XdrDataFactory;
import xdr.locallex.model.XdrData_Cdr_Quality;
import xdr.locallex.model.XdrData_Cdr_Sv;
import xdr.locallex.model.XdrData_Http;
import xdr.locallex.model.XdrData_Ims_Mo;
import xdr.locallex.model.XdrData_Ims_Mt;
import xdr.locallex.model.XdrData_MOS_BeiJing;
import xdr.locallex.model.XdrData_Mme;
import xdr.locallex.model.XdrData_Mw;
import xdr.locallex.model.XdrData_Rx;
import xdr.locallex.model.XdrData_Sv;
import xdr.locallex.model.XdrData_WJTDH_BeiJing;

public class TestAdapter
{

	public static void main(String[] args)
	{
//		String outPath = "G:\\eventstat\\VOLTEXDR指标\\新的数据\\ims_mt.txt";
		String outPath = "C:\\Users\\Administrator\\Desktop\\beijing\\测试原来的1028\\mwloc.txt";
//		String inputPath = "G:\\eventstat\\VOLTEXDR指标\\新的数据\\cdr_ims_mt_call_leg_sip_wangyou_20171020153709_339_97.csv";
//		String inputPath = "C:/Users/mastercom/Desktop/新建文件夹/dw_gprs_dpi_e_push_detail_2017072112.dat";
		String inputPath = "E:/文件/青海/青海/原始数据/XDR字段/zte_mw_20171105000819_0000.csv";
		try
		{
			FileInputStream fis = new FileInputStream(inputPath);
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			StringBuffer sbf = new StringBuffer();
			boolean[] checkStat = new boolean[20];
			while ((line = br.readLine()) != null)
			{
				try
				{
					XdrDataBase tmpItem = null;
					try
					{
						tmpItem = XdrDataFactory.GetInstance().getXdrDataObject(XdrDataFactory.XDR_DATATYPE_MW);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}

					ParseItem curParseItem = tmpItem.getDataParseItem();

					DataAdapterReader curDataAdapterReader = new DataAdapterReader(curParseItem);

					String[] strs = line.toString().split(curParseItem.getSplitMark(), -1);

					curDataAdapterReader.readData(strs);

					boolean flag =  tmpItem.FillData(curDataAdapterReader);
					if(flag==false){
						continue;
					}

					XdrData_Mw xdrDataBaseItem = (XdrData_Mw) tmpItem;

					testToEvent(checkStat, xdrDataBaseItem);
//
//					makeLocItemData(sbf, tmpItem);
					
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}

			}

			/**
			 * 输出有几个字段统计有值
			 */
			for (int k = 0; k < checkStat.length; k++)
			{
				System.out.println("fvalue[" + k + "]: " + checkStat[k]);
			}

			System.exit(0);

			// 输出位置库数据
			outPutLocationItem(outPath, sbf);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	private static void outPutLocationItem(String outPath, StringBuffer sbf) throws IOException
	{
		File file = new File(outPath);
		if (!file.exists())
		{
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(sbf.toString());
		bw.close();
	}

	/**
	 * 制造位置库数据
	 * 
	 * @param sbf
	 * @param tmpItem
	 */
	private static void makeLocItemData(StringBuffer sbf, XdrDataBase tmpItem)
	{
		if (tmpItem.imsi > 0 && tmpItem.istime > 0)
		{
			System.out.println("imsi: " + tmpItem.imsi + " time: " + tmpItem.istime);
			
			LocItem item = new LocItem();
			item.IMSI = tmpItem.imsi; // imsi
			item.itime = tmpItem.istime;
			item.ilongitude = 1140000000;
			item.ilatitude = 320000000;
			item.doorType = new Random().nextInt(2) + 1;
			item.locSource = new Random().nextInt(3) + 1;
			item.testType = new Random().nextInt(3) + 1;
			item.cityID = 1505;
			item.LteScRSRP = 66;
			item.LteScSinrUL = 65;
			// item.testType = StaticConfig.TestType_HiRail; //高铁的数据
			item.iAreaType = new Random().nextInt(5) + 1;
			item.iAreaID = new Random().nextInt(5) + 1;
			item.confidentType = new Random().nextInt(6) + 1;
			item.ibuildid = new Random().nextInt(5) - 2;
			sbf.append(item.toString() + "\n");
		}
		else if (tmpItem.imsi <= 0 && tmpItem.s1apid > 0)
		{
			System.out.println("s1apid: " + tmpItem.s1apid + " time: " + tmpItem.istime+" ecgi: "+tmpItem.ecgi);
			
			LocItem item = new LocItem();
			item.s1apid = tmpItem.s1apid;
//			item.ecgi = tmpItem.ecgi;
			item.itime = tmpItem.istime;
			item.ilongitude = 1140000000;
			item.ilatitude = 320000000;
			item.doorType = new Random().nextInt(2) + 1;
			item.locSource = new Random().nextInt(3) + 1;
			item.testType = new Random().nextInt(3) + 1;
			item.cityID = 1505;
			item.LteScRSRP = 66;
			item.LteScSinrUL = 65;
			// item.testType = StaticConfig.TestType_HiRail; //高铁的数据
			item.iAreaType = new Random().nextInt(5) + 1;
			item.iAreaID = new Random().nextInt(5) + 1;
			item.confidentType = new Random().nextInt(6) + 1;
			item.ibuildid = new Random().nextInt(5) - 2;
			sbf.append(item.toString() + "\n");
		}

	}

	/**
	 * 测试 toEvent有没有数据
	 * 
	 * @param checkStat
	 * @param mw
	 */
	private static void testToEvent(boolean[] checkStat, XdrDataBase xdrDataBase)
	{
		ArrayList<EventData> eventDataList = xdrDataBase.toEventData();

		if (eventDataList!=null && eventDataList.size() > 0)
		{
			for (int j = 0; j < 20; j++)
			{
				EventDataStruct eventStat = eventDataList.get(0).eventStat;
				if (eventStat != null && eventStat.fvalue[j] > 0)
				{
					checkStat[j] = true;
				}

			}
		}
	}

}
