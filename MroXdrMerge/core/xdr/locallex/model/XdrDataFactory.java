package xdr.locallex.model;

import java.util.HashMap;
import java.util.Map;

import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class XdrDataFactory
{

	public static int LOCTYPE_XDRLOC = 1;
	public static int LOCTYPE_MRLOC = 2;
	
	public static int XDR_DATATYPE_HTTP = 101;
	public static int XDR_DATATYPE_MOS_BEIJING = 102;
	public static int XDR_DATATYPE_WJTDH_BEIJING = 103;
	public static int XDR_DATATYPE_MW=104;
	public static int XDR_DATATYPE_SV=105;
	public static int XDR_DATATYPE_RX=106;
	
	public static int XDR_DATATYPE_MME = 107;
	public static int XDR_DATATYPE_MG = 108;
	public static int XDR_DATATYPE_RTP = 109;
	
	public static int XDR_DATATYPE_IMS_MO = 110; 
	public static int XDR_DATATYPE_IMS_MT = 111;
	public static int XDR_DATATYPE_CDR_SV = 112;
	public static int XDR_DATATYPE_CDR_QUALITY = 113;
	public static int XDR_DATATYPE_Uu = 114;
	
	

	private Map<Integer, XdrDataBase> dataTypeMap;

	private static XdrDataFactory instance;

	public static XdrDataFactory GetInstance()
	{
		if (instance == null)
		{
			instance = new XdrDataFactory();
		}
		return instance;
	}

	private XdrDataFactory()
	{
		init();
	}

	public boolean init()
	{
		dataTypeMap = new HashMap<Integer, XdrDataBase>();
		
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) || MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.QingHai)){
			{
				XdrData_Http item = new XdrData_Http();
				item.setDataType(XDR_DATATYPE_HTTP);
				dataTypeMap.put(item.getDataType(), item);
				
				XdrData_Mw itemMw = new XdrData_Mw();
				itemMw.setDataType(XDR_DATATYPE_MW);
				dataTypeMap.put(itemMw.getDataType(), itemMw);
				
				XdrData_Sv itemSv = new XdrData_Sv();
				itemSv.setDataType(XDR_DATATYPE_SV);
				dataTypeMap.put(itemSv.getDataType(), itemSv);
				
				XdrData_Rx itemRx = new XdrData_Rx();
				itemRx.setDataType(XDR_DATATYPE_RX);
				dataTypeMap.put(itemRx.getDataType(), itemRx);
				
				XdrData_Mme itemmme = new XdrData_Mme();
				itemmme.setDataType(XDR_DATATYPE_MME);
				dataTypeMap.put(itemmme.getDataType(), itemmme);
			}

			{
				XdrData_MOS_BeiJing item = new XdrData_MOS_BeiJing();
				item.setDataType(XDR_DATATYPE_MOS_BEIJING);
				dataTypeMap.put(item.getDataType(), item);
			}
			
			{
				XdrData_WJTDH_BeiJing item = new XdrData_WJTDH_BeiJing();
				item.setDataType(XDR_DATATYPE_WJTDH_BEIJING);
				dataTypeMap.put(item.getDataType(), item);
			}
			
			
			{
				XdrData_Ims_Mo itemMo = new XdrData_Ims_Mo();
				itemMo.setDataType(XDR_DATATYPE_IMS_MO);
				dataTypeMap.put(itemMo.getDataType(), itemMo);
				
				XdrData_Ims_Mt itemMt = new XdrData_Ims_Mt();
				itemMt.setDataType(XDR_DATATYPE_IMS_MT);
				dataTypeMap.put(itemMt.getDataType(), itemMt);
				
				XdrData_Cdr_Sv itemCdrSv = new XdrData_Cdr_Sv();
				itemCdrSv.setDataType(XDR_DATATYPE_CDR_SV);
				dataTypeMap.put(itemCdrSv.getDataType(), itemCdrSv);
				
				XdrData_Cdr_Quality itemCdrQuality = new XdrData_Cdr_Quality();
				itemCdrQuality.setDataType(XDR_DATATYPE_CDR_QUALITY);
				dataTypeMap.put(itemCdrQuality.getDataType(), itemCdrQuality);
			}
			
		}
		
		//内蒙数据
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng)){
					
			XdrData_Mme itemmme = new XdrData_Mme();
			itemmme.setDataType(XDR_DATATYPE_MME);
			dataTypeMap.put(itemmme.getDataType(), itemmme);
			
			XdrData_Http item_Http = new XdrData_Http();
			item_Http.setDataType(XDR_DATATYPE_HTTP);
			dataTypeMap.put(item_Http.getDataType(), item_Http);
			
			XdrData_Mg itemMg = new XdrData_Mg();
			itemMg.setDataType(XDR_DATATYPE_MG);
			dataTypeMap.put(itemMg.getDataType(), itemMg);
			
			XdrData_Sv itemNeimengSv = new XdrData_Sv();
			itemNeimengSv.setDataType(XDR_DATATYPE_SV);
			dataTypeMap.put(itemNeimengSv.getDataType(), itemNeimengSv);
			
			XdrData_Rtp itemRtp = new XdrData_Rtp();
			itemRtp.setDataType(XDR_DATATYPE_RTP);
			dataTypeMap.put(itemRtp.getDataType(), itemRtp);
		}
		
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan)){
			
			XdrData_Mme itemmme = new XdrData_Mme();
			itemmme.setDataType(XDR_DATATYPE_MME);
			dataTypeMap.put(itemmme.getDataType(), itemmme);
			
			XdrData_Http item_Http = new XdrData_Http();
			item_Http.setDataType(XDR_DATATYPE_HTTP);
			dataTypeMap.put(item_Http.getDataType(), item_Http);
			
			XdrData_Uu itemUu = new XdrData_Uu();
			itemUu.setDataType(XDR_DATATYPE_Uu);
			dataTypeMap.put(itemUu.getDataType(), itemUu);
		}
	
		return true;
	}

	
	public XdrDataBase getXdrDataObject(int dataType) throws InstantiationException, IllegalAccessException
	{
		XdrDataBase xdrData = dataTypeMap.get(dataType);
		XdrDataBase newItem = xdrData.getClass().newInstance();
		newItem.setDataType(xdrData.getDataType());
		return newItem;
	}

}
