package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import jan.util.DataAdapterReader;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;

public class XdrData_Ims_Mt extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;

	public static List<Integer> ACCESS_TYPE_List = Arrays.asList(1, 2, 43);
	public static List<Integer> RESPONSE_CODE_List = Arrays.asList(1, 403, 404, 405, 413, 414, 415, 416, 422, 423, 480,
			486, 487, 488, 600, 603, 604, 606, 1000, 2);

	// 原始的数据
	private long LAST_MME_S1APID;
	private String LAST_LTE_ECGI;

	private String P_CSCF_ID;
	private int SERVICE_TYPE; //
	private String ANSWER_TIME;
	private int ABORT_FLAG;// 中断标志，应该也是int才对

	private int ACCESS_TYPE;// 接入类型
	
	//2017-10-30
	private String RESPONSE_CODE;
	private String ALERTING_TIME;

	// 统计指标
	private int VoLTE语音终呼应答次数;
	private int VoLTE语音终呼掉话次数;


	// 异常事件指标
	private int VoLTE语音网络未接通;
	private int VoLTE语音始呼掉话;
	private int VoLTE语音终呼掉话;
	
	public XdrData_Ims_Mt()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-IMS_MT");
		}
	}

	public void clear()
	{
		// TODO
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			s1apid = dataAdapterReader.GetLongValue("LAST_MME_S1APID", StaticConfig.Int_Abnormal);

			try
			{
				String strEci = dataAdapterReader.GetStrValue("LAST_LTE_ECGI", "");
				if (strEci == null || strEci.length() == 0 || strEci.contains("."))
				{
					return false;
				}
				if (strEci.startsWith("46000"))
				{
					strEci = strEci.replace("46000", "");
				}
				if(strEci.length()!=7){
					return false;
				}
				
				String enbIDStr = strEci.substring(0,5);
				String cellIDStr = strEci.substring(5,7);
				
				int enbID = Integer.valueOf(enbIDStr, 16);
				int cellID = Integer.valueOf(cellIDStr,16); 
				
				
				ecgi = enbID*256+cellID;
			}
			catch (Exception e)
			{
				return false;
			}

			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
		}
		catch (Exception e)
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
			{
				e.printStackTrace();
			}
			return false;
		}

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			s1apid = dataAdapterReader.GetLongValue("LAST_MME_S1APID", StaticConfig.Int_Abnormal);

			try
			{
				String strEci = dataAdapterReader.GetStrValue("LAST_LTE_ECGI", "");
				if (strEci == null || strEci.length() == 0 || strEci.contains("."))
				{
					return false;
				}
				if (strEci.startsWith("46000"))
				{
					strEci = strEci.replace("46000", "");
				}
				if(strEci.length()!=7){
					return false;
				}
				
				String enbIDStr = strEci.substring(0,5);
				String cellIDStr = strEci.substring(5,7);
				
				int enbID = Integer.valueOf(enbIDStr, 16);
				int cellID = Integer.valueOf(cellIDStr,16); 
				
				
				ecgi = enbID*256+cellID;
			}
			catch (Exception e)
			{
				return false;
			}


			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);

			P_CSCF_ID = dataAdapterReader.GetStrValue("P_CSCF_ID", null);
			SERVICE_TYPE = dataAdapterReader.GetIntValue("SERVICE_TYPE", StaticConfig.Int_Abnormal);
			ANSWER_TIME = dataAdapterReader.GetStrValue("ANSWER_TIME", null);
			ABORT_FLAG = dataAdapterReader.GetIntValue("ABORT_FLAG", StaticConfig.Int_Abnormal);
			ACCESS_TYPE = dataAdapterReader.GetIntValue("ACCESS_TYPE", StaticConfig.Int_Abnormal);

			ALERTING_TIME = dataAdapterReader.GetStrValue("ALERTING_TIME", null);
			RESPONSE_CODE = dataAdapterReader.GetStrValue("RESPONSE_CODE", "");

			return true;
		}
		catch (Exception e)
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
			{
				e.printStackTrace();
			}

			return false;
		}

	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();

		if (ACCESS_TYPE_List.contains(ACCESS_TYPE) && SERVICE_TYPE == 0 && ANSWER_TIME != null && P_CSCF_ID != null)
		{
			VoLTE语音终呼应答次数 = 1;
		}
		if (P_CSCF_ID != null && SERVICE_TYPE == 0 && ABORT_FLAG == 0 && ANSWER_TIME != null
				&& ACCESS_TYPE_List.contains(ACCESS_TYPE))
		{
			VoLTE语音终呼掉话次数 = 1;
		}
		
		//异常事件 TODO zhaikaishun
		String exceptionStr = "";
		if (SERVICE_TYPE == 0 && P_CSCF_ID != null && ACCESS_TYPE_List.contains(ACCESS_TYPE)
				&& (ANSWER_TIME == null && ALERTING_TIME == null && !RESPONSE_CODE_List.contains(RESPONSE_CODE)))
		{
			VoLTE语音网络未接通 = 1;
			exceptionStr = "VoLTE语音网络未接通";
		}
		if (P_CSCF_ID != null && SERVICE_TYPE == 0 && ABORT_FLAG == 0 && ANSWER_TIME != null
				&& ACCESS_TYPE_List.contains(ACCESS_TYPE))
		{
			VoLTE语音始呼掉话 = 1;
			exceptionStr = "VoLTE语音始呼掉话";
		}
		if (P_CSCF_ID != null && SERVICE_TYPE == 0 && ABORT_FLAG == 0 && ANSWER_TIME != null
				&& ACCESS_TYPE_List.contains(ACCESS_TYPE))
		{
			VoLTE语音终呼掉话 = 1;
			exceptionStr = "VoLTE语音终呼掉话";
		}
		

		
		EventData eventData = new EventData();
		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = (int) ecgi;
		eventData.iTime = istime;
		eventData.wTimems = 0;
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.Interface = StaticConfig.INTERFACE_NEW_S1U;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;

		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		// event stat
		eventData.eventStat = new EventDataStruct();

		eventData.eventStat.fvalue[4] = VoLTE语音终呼应答次数;
		eventData.eventStat.fvalue[5] = VoLTE语音终呼掉话次数;
		
		if(exceptionStr.length()>0){
			eventData.eventDetial.strvalue[0]=exceptionStr;
			eventData.eventDetial.fvalue[0]=LteScRSRP;
			eventData.eventDetial.fvalue[1]=LteScSinrUL;
		}
		
		eventData.eventDetial = null;
		eventDataList.add(eventData);
		
		
		
		
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		// TODO Auto-generated method stub

	}

}
