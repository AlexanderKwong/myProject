package xdr.locallex.model;

import jan.util.DataAdapterConf.ParseItem;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;
import jan.util.DataAdapterReader;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import StructData.StaticConfig;

public class HttpPage extends XdrDataBase
{
	public long Procedure_Start_Time;
	public long a_Procedure_End_time;
	public long Procedure_End_Time;
	public int App_Typ;
	public int App_Status = -1;
	public long DL_Data;
	public long 最后一条话单的HTTP最后一个内容包的时间点;
	public long 最后一条话单的HTTP最后一个Ack包的时间点;
	public String HOST;
	public String URI;
	public String Refer_URI;
	public int longitude;
	public int latitude;
	public int 合并话单数;
	public int HTTP响应成功会话数;
	public int HTTP传输完成会话数;

	// 用来判断的
	public long lastEndTime;
	public String URL;
	public int ibuildheight;
	public int Eci;
	public int Interface;

	// 用来统计的
	public int 显示时长大于5秒次数;

	public void loadData(XdrData_Http xdrData_Http)
	{
		istimems = xdrData_Http.istimems;

		Procedure_Start_Time = xdrData_Http.istime * 1000L + istimems;
		a_Procedure_End_time = xdrData_Http.ietime * 1000L + ietimems;
		App_Typ = xdrData_Http.App_Type;

		HOST = xdrData_Http.HOST;
		URI = xdrData_Http.URI;
		Refer_URI = xdrData_Http.Refer_URI;
		longitude = xdrData_Http.iLongitude;
		latitude = xdrData_Http.iLatitude;

		// 其他的几个
		iCityID = xdrData_Http.iCityID;
		istime = xdrData_Http.istime;
		strloctp = "";
		label = xdrData_Http.label;
		ibuildid = xdrData_Http.ibuildid;

		ibuildheight = xdrData_Http.iheight;

		imsi = xdrData_Http.imsi;
		Eci = (int) xdrData_Http.Eci;

		Interface = StaticConfig.INTERFACE_S1_U;

		testType = xdrData_Http.testType;
		iDoorType = xdrData_Http.iDoorType;
		locSource = xdrData_Http.locSource;

		confidentType = xdrData_Http.confidentType;
		iAreaType = xdrData_Http.iAreaType;
		iAreaID = xdrData_Http.iAreaID;
	}

	public void statData(XdrData_Http xdrData_Http)
	{
		Procedure_End_Time = xdrData_Http.ietime * 1000L + xdrData_Http.ietimems;
		DL_Data = DL_Data + xdrData_Http.DL_Data;

		if (longitude <= 0 && xdrData_Http.iLongitude > 0)
		{
			longitude = xdrData_Http.iLongitude;
			latitude = xdrData_Http.iLatitude;
			ibuildid = xdrData_Http.ibuildid;
			ibuildheight = xdrData_Http.iheight;
			testType = xdrData_Http.testType;
			iDoorType = xdrData_Http.iDoorType;
			locSource = xdrData_Http.locSource;
			confidentType = xdrData_Http.confidentType;
			iAreaType = xdrData_Http.iAreaType;
			iAreaID = xdrData_Http.iAreaID;
		}

		最后一条话单的HTTP最后一个内容包的时间点 = (xdrData_Http.istime * 1000L + xdrData_Http.istimems)
				+ xdrData_Http.last_http_resp_delay_ms;
		
		
		最后一条话单的HTTP最后一个Ack包的时间点 = (xdrData_Http.istime * 1000L + xdrData_Http.istimems)
				+ xdrData_Http.last_ack_delay_ms;
		合并话单数++;
		if (xdrData_Http.HTTP_WAP_STATUS > 0 && xdrData_Http.HTTP_WAP_STATUS < 400 && xdrData_Http.HTTP_WAP_STATUS >0)
		{
			HTTP响应成功会话数++;
		}
		if (xdrData_Http.HTTP_WAP_STATUS > 0 && xdrData_Http.HTTP_WAP_STATUS < 400 
				&& xdrData_Http.HTTP_WAP_STATUS > 0 && xdrData_Http.businessFinishMark==3)// && 2017-10-26我又加上后面的一位了
																					// xdrData_Http.SESSION_MARK_END==3)
		{
			HTTP传输完成会话数++;
		}

		// 记录上一次的时间点
		lastEndTime = xdrData_Http.ietime * 1000L + xdrData_Http.ietimems;
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{

		return null;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{

		return false;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{

		return false;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		if (HTTP传输完成会话数 / (合并话单数 * 1.0) >= 0.9)
		{
			App_Status = 0;
		}
		else
		{
			App_Status = -1;
		}
		if (最后一条话单的HTTP最后一个内容包的时间点 - Procedure_Start_Time > 5000)
		{
			显示时长大于5秒次数 = 1;
		}

		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		EventData eventData = new EventData();
		eventData.eventStat = new EventDataStruct();
		eventData.iCityID = iCityID;
		eventData.iTime = istime;
		eventData.wTimems = istimems;
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = longitude;
		eventData.iLatitude = latitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = ibuildheight;
		eventData.IMSI = imsi;
		eventData.iEci = Eci;
		eventData.Interface = Interface;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		// 网页显示时长
		eventData.eventStat.fvalue[8] = (最后一条话单的HTTP最后一个内容包的时间点 - Procedure_Start_Time) / 1000.0;
		// 网页请求次数
		eventData.eventStat.fvalue[9] = 1;
		// 显示时长大于5秒次数
		eventData.eventStat.fvalue[10] = 显示时长大于5秒次数;
		// 显示成功次数
		if (App_Status == 0)
		{
			eventData.eventStat.fvalue[11] = 1;
		}
		else
		{
			eventData.eventStat.fvalue[11] = 0;
		}
		// DLData下载字节数
		eventData.eventStat.fvalue[12] = DL_Data;

		eventDataList.add(eventData);
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{

	}
}
