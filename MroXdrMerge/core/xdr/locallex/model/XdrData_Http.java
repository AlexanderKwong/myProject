package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import StructData.GridItem;
import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;

public class XdrData_Http extends XdrDataBase
{

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	private Date tmDate = new Date();
	private static ParseItem parseItem;

	public int City;
	public String XDR_ID;
	public String MSISDN;
	public String IMEI;
	public int TAC;
	public long Cell_ID;
	public long Eci;
	public String APN;
	public String App_Type_Code;
	public int App_Type;
	public int App_Sub_type;
	public int App_Status;
	public long UL_Data;
	public long DL_Data;
	public long UL_IP_Packet;
	public long DL_IP_Packet;
	public long tcp_suc_first_req_delay_ms;
	public long first_req_first_resp_delay_ms;
	public int req_type;
	public long last_http_resp_delay_ms;
	public long last_ack_delay_ms;
	public String HOST;
	public int TCP_RESPONSE_DELAY;
	public int TCP_CONFIRM_DELAY;
	public int TCP_ATT_CNT;
	public int TCP_CONN_STATUS;
	public int SESSION_MARK_END;
	public int TRANSACTION_TYPE;
	public int HTTP_WAP_STATUS;
	public int FIRST_HTTP_RES_DELAY;
	public int last_http_content_delay_ms;
	public int WTP_INTERRUPT_TYPE;
	
	public int businessFinishMark;

	public double IPThroughputUL;
	public double IPThroughputDL;

	// 统计的指标 2017-07-24 zhaikaishun加
	public long HTTP_latency;
	public long HTTP_Accept;
	public long traffic_ip_all;
	public long trans_delay1;
	public int HTTP_ATTEMPT;
	public int TCP_ATTEMPT;
	public int TCP_Accept;
	public long TCP_latency;

	// 话单统计
	public String HTTP_content_type;
	public String URI;
	public String Refer_URI;
	
	private StringBuffer value; 

	public XdrData_Http()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-S1-HTTP");
		}
	}

	public void clear()
	{
		City = StaticConfig.Int_Abnormal;
		XDR_ID = "";
		MSISDN = "";
		IMEI = "";
		TAC = StaticConfig.Int_Abnormal;
		Cell_ID = StaticConfig.Int_Abnormal;
		Eci = StaticConfig.Int_Abnormal;
		APN = "";
		App_Type_Code = "";
		App_Type = StaticConfig.Int_Abnormal;
		App_Sub_type = StaticConfig.Int_Abnormal;
		App_Status = StaticConfig.Int_Abnormal;
		UL_Data = StaticConfig.Int_Abnormal;
		DL_Data = StaticConfig.Int_Abnormal;
		UL_IP_Packet = StaticConfig.Int_Abnormal;
		DL_IP_Packet = StaticConfig.Int_Abnormal;
		tcp_suc_first_req_delay_ms = 0;
		first_req_first_resp_delay_ms = 0;
		req_type = StaticConfig.Int_Abnormal;
		last_http_resp_delay_ms = 0;
		last_ack_delay_ms = 0;
		HOST = "";
		TCP_RESPONSE_DELAY = StaticConfig.Int_Abnormal;
		TCP_CONFIRM_DELAY = StaticConfig.Int_Abnormal;
		TCP_ATT_CNT = StaticConfig.Int_Abnormal;
		TCP_CONN_STATUS = StaticConfig.Int_Abnormal;
		SESSION_MARK_END = StaticConfig.Int_Abnormal;
		TRANSACTION_TYPE = StaticConfig.Int_Abnormal;
		HTTP_WAP_STATUS = StaticConfig.Int_Abnormal;
		FIRST_HTTP_RES_DELAY = StaticConfig.Int_Abnormal;
		last_http_content_delay_ms = StaticConfig.Int_Abnormal;
		WTP_INTERRUPT_TYPE = StaticConfig.Int_Abnormal;
		businessFinishMark = StaticConfig.Int_Abnormal;
		HTTP_content_type = "";
		URI = "";
		Refer_URI = "";
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
			// stime
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			Cell_ID = dataAdapterReader.GetLongValue("Cell_ID", 0);
			Eci = Cell_ID;

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);
			value = dataAdapterReader.getTmStrs();

		}
		catch (Exception e)
		{
			throw e;
		}

		try
		{
			City = dataAdapterReader.GetIntValue("City", 0);
			XDR_ID = dataAdapterReader.GetStrValue("XDR_ID", "");
			// zhaikaishun
			imsi = dataAdapterReader.GetLongValue("IMSI", 0L);
			IMEI = dataAdapterReader.GetStrValue("IMEI", "");
			MSISDN = dataAdapterReader.GetStrValue("MSISDN", "");
			TAC = dataAdapterReader.GetIntValue("TAC", 0);
			Cell_ID = dataAdapterReader.GetLongValue("Cell_ID", 0);
			Eci = Cell_ID;
			APN = dataAdapterReader.GetStrValue("APN", "");
			App_Type_Code = dataAdapterReader.GetStrValue("App_Type_Code", "");
			App_Type = dataAdapterReader.GetIntValue("App_Type", StaticConfig.Int_Abnormal);
			App_Sub_type = dataAdapterReader.GetIntValue("App_Sub_type", StaticConfig.Int_Abnormal);
			App_Status = dataAdapterReader.GetIntValue("App_Status", StaticConfig.Int_Abnormal);
			UL_Data = dataAdapterReader.GetLongValue("UL_Data", 0);
			DL_Data = dataAdapterReader.GetLongValue("DL_Data", 0);
			UL_IP_Packet = dataAdapterReader.GetLongValue("UL_IP_Packet", 0);
			DL_IP_Packet = dataAdapterReader.GetLongValue("DL_IP_Packet", 0);
			tcp_suc_first_req_delay_ms = dataAdapterReader.GetLongValue("tcp_suc_first_req_delay_ms", 0);
			first_req_first_resp_delay_ms = dataAdapterReader.GetLongValue("first_req_first_resp_delay_ms", 0);
			req_type = dataAdapterReader.GetIntValue("req_type", 0);
			last_http_resp_delay_ms = dataAdapterReader.GetLongValue("last_http_resp_delay_ms", 0);
			last_ack_delay_ms = dataAdapterReader.GetLongValue("last_ack_delay_ms", 0);
			HOST = dataAdapterReader.GetStrValue("HOST", "");
			TCP_RESPONSE_DELAY = dataAdapterReader.GetIntValue("TCP_RESPONSE_DELAY", 0);
			TCP_CONFIRM_DELAY = dataAdapterReader.GetIntValue("TCP_CONFIRM_DELAY", 0);
			TCP_ATT_CNT = dataAdapterReader.GetIntValue("TCP_ATT_CNT", StaticConfig.Int_Abnormal);
			TCP_CONN_STATUS = dataAdapterReader.GetIntValue("TCP_CONN_STATUS", StaticConfig.Int_Abnormal);
			SESSION_MARK_END = dataAdapterReader.GetIntValue("SESSION_MARK_END", StaticConfig.Int_Abnormal);
			TRANSACTION_TYPE = dataAdapterReader.GetIntValue("TRANSACTION_TYPE", StaticConfig.Int_Abnormal);
			HTTP_WAP_STATUS = dataAdapterReader.GetIntValue("HTTP_WAP_STATUS", StaticConfig.Int_Abnormal);
			FIRST_HTTP_RES_DELAY = dataAdapterReader.GetIntValue("FIRST_HTTP_RES_DELAY", 0);
			last_http_content_delay_ms = dataAdapterReader.GetIntValue("last_http_content_delay_ms", StaticConfig.Int_Abnormal);
			WTP_INTERRUPT_TYPE = dataAdapterReader.GetIntValue("WTP_INTERRUPT_TYPE", StaticConfig.Int_Abnormal);
			HTTP_content_type = dataAdapterReader.GetStrValue("HTTP_content_type", "");
			URI = dataAdapterReader.GetStrValue("URI", "");
			Refer_URI = dataAdapterReader.GetStrValue("Refer_URI", "");
			if (App_Status == 1)
			{
				if (FIRST_HTTP_RES_DELAY > 0)
				{
					IPThroughputUL = (double) (UL_Data * 8.0 / (FIRST_HTTP_RES_DELAY / 1000.0)) / 1024;
					IPThroughputDL = (double) (DL_Data * 8.0 / (FIRST_HTTP_RES_DELAY / 1000.0)) / 1024;
				}
			}
			try{
				businessFinishMark = dataAdapterReader.GetIntValue("businessFinishMark",3);// 业务完成标识
			}catch(Exception e){
				businessFinishMark = 3;
			}
			

		}
		catch (Exception e)
		{
			// e.printStackTrace();
		}

		return true;
	}

	@Override
	public String toString()
	{
		return "SIGNAL_XDR_HTTP [d_beginTime=" + ", format=" + format + ", City=" + City + ", XDR_ID=" + XDR_ID + ", IMSI=" + imsi + ", MSISDN=" + MSISDN
				+ ", IMEI=" + IMEI + ", TAC=" + TAC + ", Cell_ID=" + Cell_ID + ", Eci=" + Eci + ", APN=" + APN + ", App_Type_Code=" + App_Type_Code + ", stime=" + istime + ", etime=" + ietime
				+ ", App_Type=" + App_Type + ", App_Sub_type=" + App_Sub_type + ", App_Status=" + App_Status + ", UL_Data=" + UL_Data + ", DL_Data=" + DL_Data + ", UL_IP_Packet=" + UL_IP_Packet
				+ ", DL_IP_Packet=" + DL_IP_Packet + ", tcp_suc_first_req_delay_ms=" + tcp_suc_first_req_delay_ms + ", first_req_first_resp_delay_ms=" + first_req_first_resp_delay_ms + ", req_type="
				+ req_type + ", last_http_resp_delay_ms=" + last_http_resp_delay_ms + ", last_ack_delay_ms=" + last_ack_delay_ms + ", HOST=" + HOST + ", TCP_RESPONSE_DELAY=" + TCP_RESPONSE_DELAY
				+ ", TCP_CONFIRM_DELAY=" + TCP_CONFIRM_DELAY + ", TCP_ATT_CNT=" + TCP_ATT_CNT + ", TCP_CONN_STATUS=" + TCP_CONN_STATUS + ", SESSION_MARK_END=" + SESSION_MARK_END
				+ ", TRANSACTION_TYPE=" + TRANSACTION_TYPE + ", HTTP_WAP_STATUS=" + HTTP_WAP_STATUS + ", FIRST_HTTP_RES_DELAY=" + FIRST_HTTP_RES_DELAY + ", last_http_content_delay_ms="
				+ last_http_content_delay_ms + ", WTP_INTERRUPT_TYPE=" + WTP_INTERRUPT_TYPE + ", IPThroughputUL=" + IPThroughputUL + ", IPThroughputDL=" + IPThroughputDL + ", ilongitude=" + iLongitude
				+ ", ilatitude=" + iLatitude + ", ibuildid=" + ibuildid + ", iheight=" + iheight + ", testType=" + testType + ", doorType=" + iDoorType + ", radius=" + iRadius + ", loctp=" + strloctp
				+ ", label=" + label + ", locSource=" + locSource + "]";
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();

		boolean haveValue = stat();
		if (!haveValue)
		{
			return null;
		}

		EventData eventData = new EventData();
		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = (int) Eci;
		eventData.iTime = istime;
		eventData.wTimems = 0;
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.Interface = StaticConfig.INTERFACE_S1_U;
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

		eventData.eventStat.fvalue[0] = HTTP_latency;
		eventData.eventStat.fvalue[1] = HTTP_Accept;
		eventData.eventStat.fvalue[2] = traffic_ip_all;
		eventData.eventStat.fvalue[3] = trans_delay1;
		eventData.eventStat.fvalue[4] = HTTP_ATTEMPT;
		eventData.eventStat.fvalue[5] = TCP_ATTEMPT;
		eventData.eventStat.fvalue[6] = TCP_Accept;
		eventData.eventStat.fvalue[7] = TCP_latency;

		eventDataList.add(eventData);
		return eventDataList;
	}

	public boolean stat()
	{
		boolean haveValue = false;

		if(HTTP_WAP_STATUS < 400 && HTTP_WAP_STATUS>0  &&TRANSACTION_TYPE == 6){
			HTTP_latency = HTTP_latency + last_http_content_delay_ms;
			haveValue = true;
		}
		
		
		if(HTTP_WAP_STATUS < 400 && HTTP_WAP_STATUS>0 && TRANSACTION_TYPE == 6){
			HTTP_Accept++;
			haveValue = true;
		}
		

		if(TRANSACTION_TYPE == 6 && businessFinishMark ==3 && last_http_content_delay_ms < 300000)
		{
			traffic_ip_all = traffic_ip_all + DL_Data;
			haveValue = true;
		}
		

		if(TRANSACTION_TYPE == 6 && businessFinishMark ==3 && last_http_content_delay_ms < 300000){
			trans_delay1 = trans_delay1 + last_http_content_delay_ms;
			haveValue = true;
		}
		
		if(TRANSACTION_TYPE == 6){
			HTTP_ATTEMPT++;
			haveValue = true;
		}
		
		if (TCP_ATT_CNT > 0)
		{
			TCP_ATTEMPT++;
			haveValue = true;
		}

		if (TCP_ATT_CNT > 0 && TCP_CONN_STATUS == 0)
		{
			TCP_Accept++;
			haveValue = true;
		}
		

		if (TCP_CONN_STATUS == 0)
		{
			TCP_latency = TCP_latency + TCP_CONFIRM_DELAY;//+ TCP_RESPONSE_DELAY;
			haveValue = true;
		}
		return haveValue;
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
			sb.append(StaticConfig.cityId_Name.get(iCityID)+"_"+icentLng+"_"+icentLat);sb.append(fenge); 
		}else {
			sb.append("nocity"+"_"+icentLng+"_"+icentLat);sb.append(fenge);
		}
		
		sb.append(-1);sb.append(fenge);
		sb.append(-1);
	
	}

}
