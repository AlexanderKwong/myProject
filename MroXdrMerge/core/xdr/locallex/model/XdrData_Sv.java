package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import StructData.GridItem;
import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;
import jan.util.DataAdapterReader;

public class XdrData_Sv extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	public int Interface;
	public long XDR_ID;
	public String IMEI;
	public String IMSI;
	public int ProceDure_Type;

	public int REQUEST_RESULT;
	public int RESULT;
	public int SV_CAUSE;
	public int POST_FAILURE_CAUSE;
	public long RESP_DELAY;
	public int SV_DELAY;
	public int ECI;
	private StringBuffer value; 

	// 统计字段
	private long SRVCC切换请求次数 = StaticConfig.Int_Abnormal;
	private long SRVCC切换成功次数 = StaticConfig.Int_Abnormal;
	
	//detail字段
	private long SRVCC切换失败 = StaticConfig.Int_Abnormal;

	public XdrData_Sv()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Sv");
		}
	}

	public void clear()
	{

		Interface = StaticConfig.Int_Abnormal;
		;
		XDR_ID = StaticConfig.Int_Abnormal;
		IMEI = "";
		IMSI = "";
		ProceDure_Type = StaticConfig.Int_Abnormal;

		REQUEST_RESULT = StaticConfig.Int_Abnormal;
		RESULT = StaticConfig.Int_Abnormal;
		SV_CAUSE = StaticConfig.Int_Abnormal;
		POST_FAILURE_CAUSE = StaticConfig.Int_Abnormal;
		RESP_DELAY = StaticConfig.Int_Abnormal;
		SV_DELAY = StaticConfig.Int_Abnormal;
		ECI = StaticConfig.Int_Abnormal;
		value = new StringBuffer();
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
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
		}
		catch (Exception e)
		{
			return false;
		}

		return true;

	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
		}
		catch (Exception e)
		{
			return false;
		}

		Interface = dataAdapterReader.GetIntValue("Interface", StaticConfig.Int_Abnormal);
		XDR_ID = dataAdapterReader.GetLongValue("XDR_ID", StaticConfig.Int_Abnormal);
		IMEI = dataAdapterReader.GetStrValue("IMEI", "");
		IMSI = dataAdapterReader.GetStrValue("IMSI", "");
		ProceDure_Type = dataAdapterReader.GetIntValue("ProceDure_Type", StaticConfig.Int_Abnormal);

		REQUEST_RESULT = dataAdapterReader.GetIntValue("REQUEST_RESULT", StaticConfig.Int_Abnormal);
		RESULT = dataAdapterReader.GetIntValue("RESULT", StaticConfig.Int_Abnormal);
		SV_CAUSE = dataAdapterReader.GetIntValue("SV_CAUSE", StaticConfig.Int_Abnormal);
		POST_FAILURE_CAUSE = dataAdapterReader.GetIntValue("POST_FAILURE_CAUSE", StaticConfig.Int_Abnormal);
		RESP_DELAY = dataAdapterReader.GetLongValue("RESP_DELAY", StaticConfig.Int_Abnormal);
		SV_DELAY = dataAdapterReader.GetIntValue("SV_DELAY", StaticConfig.Int_Abnormal);
		ECI = dataAdapterReader.GetIntValue("ECI", StaticConfig.Int_Abnormal);
		value = dataAdapterReader.getTmStrs();
		return true;
	}

	@Override
	public String toString()
	{
		return "";
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		EventData eventData = new EventData();

		if (ProceDure_Type == 1)
		{
			SRVCC切换请求次数 = 1;
			if (RESULT == 0)
			{
				SRVCC切换成功次数 = 1;
			}
		}
		if(SRVCC切换请求次数!=1){
//			return null;
			eventData.eventStat = null;
		}
		if(ProceDure_Type == 1 && RESULT >0){
			SRVCC切换失败 = 1;
		}else{
			eventData.eventDetial = null;
		}

		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = ECI;
		eventData.iTime = istime;
		eventData.wTimems = istimems; // 开始时间里面的毫秒
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.Interface = StaticConfig.INTERFACE_SV;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = ProceDure_Type;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		
		if(eventData.eventStat!=null){
			eventData.eventStat.fvalue[0] = SRVCC切换请求次数;
			eventData.eventStat.fvalue[1] = SRVCC切换成功次数;
		}
		if(eventData.eventDetial!=null){
			eventData.eventDetial.strvalue[0] = "SRVCC切换失败";
			eventData.eventDetial.fvalue[0]=LteScRSRP;
			eventData.eventDetial.fvalue[1] = LteScSinrUL;
			eventData.eventDetial.fvalue[2]=REQUEST_RESULT;
			eventData.eventDetial.fvalue[3]=RESULT;
			eventData.eventDetial.fvalue[4]=SV_CAUSE;
			eventData.eventDetial.fvalue[5]=POST_FAILURE_CAUSE;
			eventData.eventDetial.fvalue[6]=RESP_DELAY;
			eventData.eventDetial.fvalue[7]=SV_DELAY;
				
		}
		eventDataList.add(eventData);
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{

		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if(fenge.contains("\\")){
			fenge = fenge.replace("\\", "");
		}
		sb.append(value);
		sb.append(fenge);
		sb.append(iLongitude);sb.append(fenge);
		sb.append(iLatitude);sb.append(fenge);
		sb.append(iheight);sb.append(fenge);
		sb.append(iDoorType);sb.append(fenge);
		sb.append(iRadius);sb.append(fenge);
		GridItem gridItem  = GridItem.GetGridItem(0,iLongitude,iLatitude);
		int icentLng =gridItem.getBRLongitude()/2+gridItem.getTLLongitude()/2;
		int icentLat = gridItem.getBRLatitude()/2+gridItem.getTLLatitude()/2;
		if(StaticConfig.cityId_Name.containsKey(iCityID)){
			
		}
		if(StaticConfig.cityId_Name.containsKey(iCityID)){
			sb.append(StaticConfig.cityId_Name.get(iCityID)+"_"+icentLng+"_"+icentLat);sb.append(fenge); 
		}else {
			sb.append("nocity"+"_"+icentLng+"_"+icentLat);sb.append(fenge);
		}
		sb.append(-1);sb.append(fenge);
		sb.append(-1);
	

	}

}
