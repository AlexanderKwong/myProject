package StructData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.util.DataAdapterReader;
import jan.util.GisFunction;
import jan.util.GisLocater;
import jan.util.GisPos;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.StringUtil;

public class SIGNAL_XDR_4G extends SIGNAL_LOC
{
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

	public int fileID;

	public int etime;
	public int MME_GROUP_ID;
	public int MME_CODE;
	public long MME_UE_S1AP_ID;
	public long ENB_UE_S1AP_ID;
	public String ENB_IP;
	public String ENB;
	public int ENB_PORT;
	public int UE_IP_TYPE;
	public long UP_UL_TEID;
	public long UP_DL_TEID;
	public int TAC;
	public long CI;
	public long Eci;
	public int PCI;
	public long M_TMSI;
	public long IMSI;
	public String MSISDN;
	public String IMEI;
	public String Brand;
	public String Type;
	public int Event_Type;
	public int Event_Direction;
	public int PROTOCOL;
	public int IP_LEN_UL;
	public int IP_LEN_DL;
	public int IP_LEN;
	public int IP_Data_Len_UL;
	public int IP_Data_Len_DL;
	public int Count_Packet_UL;
	public int Count_Packet_DL;
	public int User_IP_LEN_UL;
	public int HTTP_WAP_status;
	public int User_IP_LEN_DL;
	public int Duration;
	public int Service_Type;
	public int Sub_Service_Type;
	public int Sed_Service;
	public int IsClient;
	public int Resp;
	public int Resp_DelayFirst;
	public int Resp_Cause;
	public int MMS_Cause;
	public int Result;
	public int Result_DelayFirst;// 微秒
	public int Ack;
	public int Ack_Delay;
	public int Last_Ack_Delay;
	public String Referer;
	public int Fin;
	public int fin_delay;
	public int Abort;
	public int Abort_Reason_User;
	public int Abort_Reason_Provider;
	public int Disconnect;
	public int Reset;
	public int Reset_Direction;
	public int Retran_Packet_DL;
	public int Total_Packet_DL;
	public int Retran_Packet_UL;
	public double Weight_Rate_UL;
	public double Weight_Rate_DL;
	public double Weight_Rate_Total;
	public double ip_len_dl_rate;
	public String host;

	public int LocFillType;
	private String strTime;
	private Date d_beginTime;

	public int Procedure_Status;
	// add 场景统计标识
	public int areaId = -1;
	public int areaType = -1;
	// 记录前后5分钟小区切换信息 add 20170603
	public StringBuffer eciSwitchList = new StringBuffer();

	public void cleanLoc()
	{
		this.latitude = 0;
		this.longitudeGL = 0;
		this.latitudeGL = 0;
		this.longitudeGL = 0;
	}

	public SIGNAL_XDR_4G()
	{
		Clear();

		cityID = -1;
		ENB = "";
		MSISDN = "";
		IMEI = "";
		Brand = "";
		Type = "";
		Referer = "";
		strTime = "";

		LocFillType = 0;
		radius = 10000;
		testType = -1;
		location = -1;
		dist = -1;
		indoor = -1;
		loctp = "";
		networktype = "";
		lable = "unknow";

		testTypeGL = -1;
		longitudeGL = 0;
		latitudeGL = 0;
		locationGL = 0;
		distGL = 0;
		radiusGL = 0;
		loctpGL = "";
		indoorGL = 0;
		lableGL = "unknow";

		mt_speed = StaticConfig.Int_Abnormal;
		mt_label = "unknow";

		moveDirect = -1;
		loctimeGL = 0;

		host = "";
	}

	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		cityID = -1;
		fileID = 0;

		try
		{
			strTime = dataAdapterReader.GetStrValue("start_date_time", "");
			if (strTime.indexOf(".") < 0)
			{
				strTime += ".000000";
			}
			d_beginTime = format.parse(strTime.substring(0, strTime.length() - 3));

			// d_beginTime = format.parse(strTime);
			stime = (int) (d_beginTime.getTime() / 1000L);
			stime_ms = (int) (d_beginTime.getTime() % 1000L);

			CI = dataAdapterReader.GetLongValue("ci", 0);
			Eci = CI;

			IMSI = dataAdapterReader.GetLongValue("imsi", 0);

			GisPos gisPos = GisLocater.bd09_To_Gps84(dataAdapterReader.GetDoubleValue("bd_lon", 0), dataAdapterReader.GetDoubleValue("bd_lat", 0));

			if (gisPos.getWgLon() > 180 || gisPos.getWgLon() < 0)
			{
				longitude = 0;
			}
			if (gisPos.getWgLat() > 90 || gisPos.getWgLat() < 0)
			{
				latitude = 0;
			}

			longitude = (int) (gisPos.getWgLon() * 10000000D);
			latitude = (int) (gisPos.getWgLat() * 10000000D);

			location = dataAdapterReader.GetIntValue("location", 0);
			dist = dataAdapterReader.GetLongValue("dist", 0);
			radius = dataAdapterReader.GetIntValue("radius", 0);
			loctp = dataAdapterReader.GetStrValue("loctp", "");
			indoor = dataAdapterReader.GetIntValue("indoor", 0);

			lable = "unknow";

			latlng_time = (int) (dataAdapterReader.GetLongValue("latlng_time", 0) / 1000L);
			if (latlng_time == 0)
			{
				latlng_time = stime;
			}

		}
		catch (Exception e)
		{
			throw e;
		}

		try
		{
			strTime = dataAdapterReader.GetStrValue("end_date_time", "");
			if (strTime.indexOf(".") < 0)
			{
				strTime += ".000000";
			}
			d_beginTime = format.parse(strTime.substring(0, strTime.length() - 3));
			etime = (int) (d_beginTime.getTime() / 1000L);

			Online_ID = dataAdapterReader.GetLongValue("online_id", 0);
			Session_ID = dataAdapterReader.GetLongValue("session_id", 0);

			MME_GROUP_ID = dataAdapterReader.GetIntValue("mme_group_id", 0);
			MME_CODE = dataAdapterReader.GetIntValue("mme_code", 0);
			MME_UE_S1AP_ID = dataAdapterReader.GetLongValue("mme_ue_s1ap_id", 0);
			ENB_UE_S1AP_ID = dataAdapterReader.GetLongValue("enb_ue_s1ap_id", 0);
			ENB = "";
			TAC = dataAdapterReader.GetIntValue("tac", 0);
			MSISDN = dataAdapterReader.GetStrValue("msisdn", "");
			IMEI = dataAdapterReader.GetStrValue("imei", "");
			Brand = dataAdapterReader.GetStrValue("brand", "");
			Type = dataAdapterReader.GetStrValue("type", "");
			Event_Type = dataAdapterReader.GetIntValue("event_type", 0);
			IP_LEN_UL = dataAdapterReader.GetIntValue("ip_len_ul", 0);
			IP_LEN_DL = dataAdapterReader.GetIntValue("ip_len_dl", 0);
			IP_LEN = dataAdapterReader.GetIntValue("ip_len", 0);
			IP_Data_Len_UL = dataAdapterReader.GetIntValue("ip_data_len_ul", 0);
			IP_Data_Len_DL = dataAdapterReader.GetIntValue("ip_data_len_dl", 0);
			Count_Packet_UL = dataAdapterReader.GetIntValue("count_packet_ul", 0);
			Count_Packet_DL = dataAdapterReader.GetIntValue("count_packet_dl", 0);
			User_IP_LEN_UL = dataAdapterReader.GetIntValue("user_ip_len_ul", 0);
			User_IP_LEN_DL = dataAdapterReader.GetIntValue("user_ip_len_dl", 0);
			HTTP_WAP_status = dataAdapterReader.GetIntValue("HTTP_WAP_status", 0);
			Duration = dataAdapterReader.GetIntValue("duration", 0);
			Service_Type = dataAdapterReader.GetIntValue("service_type", 0);
			Sub_Service_Type = dataAdapterReader.GetIntValue("sub_service_type", 0);
			Result = dataAdapterReader.GetIntValue("result", 0);
			Result_DelayFirst = dataAdapterReader.GetIntValue("result_delayfirst", 0);
			Referer = dataAdapterReader.GetStrValue("referer", "");
			Retran_Packet_DL = dataAdapterReader.GetIntValue("retran_packet_dl", 0);
			Retran_Packet_UL = dataAdapterReader.GetIntValue("retran_packet_ul", 0);
			Weight_Rate_UL = dataAdapterReader.GetDoubleValue("weight_rate_ul", 0);
			Weight_Rate_DL = dataAdapterReader.GetDoubleValue("weight_rate_dl", 0);
			Weight_Rate_Total = dataAdapterReader.GetDoubleValue("weight_rate_total", 0);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return true;
	}

	public boolean FillData_SEQ_MME(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		cityID = -1;
		fileID = 0;

		try
		{
			d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			Procedure_Status = dataAdapterReader.GetIntValue("Procedure_Status", 0);

			stime = (int) (d_beginTime.getTime() / 1000L);
			stime_ms = (int) (d_beginTime.getTime() % 1000L);

			CI = dataAdapterReader.GetLongValue("Cell_ID", 0);
			Eci = CI;

			IMSI = dataAdapterReader.GetLongValue("IMSI", 0);

			longitude = 0;
			latitude = 0;
		}
		catch (Exception e)
		{
			throw e;
		}

		try
		{

			etime = (int) (d_beginTime.getTime() / 1000L);

			MME_GROUP_ID = dataAdapterReader.GetIntValue("MME_Group_ID", 0);
			MME_CODE = dataAdapterReader.GetIntValue("MME_Code", 0);
			MME_UE_S1AP_ID = dataAdapterReader.GetLongValue("MME_UE_S1AP_ID", 0);
			ENB_IP = dataAdapterReader.GetStrValue("eNB_IP_Add", "");
			ENB = "";
			Event_Type = dataAdapterReader.GetIntValue("Procedure_Type", 0);
			TAC = dataAdapterReader.GetIntValue("TAC", 0);
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.MSISDN))
			{
				MSISDN = dataAdapterReader.GetStrValue("MSISDN", "");
			}
			else
			{
				MSISDN = "";
			}
			IMEI = dataAdapterReader.GetStrValue("IMEI", "");
		}
		catch (Exception e)
		{
		}

		return true;
	}

	public boolean FillData_SEQ_HTTP(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		cityID = -1;
		fileID = 0;

		try
		{
			d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			Procedure_Status = dataAdapterReader.GetIntValue("App_Status", 0);

			stime = (int) (d_beginTime.getTime() / 1000L);
			stime_ms = (int) (d_beginTime.getTime() % 1000L);

			CI = dataAdapterReader.GetLongValue("Cell_ID", 0);
			Eci = CI;

			IMSI = dataAdapterReader.GetLongValue("IMSI", 0);

			GisPos gisPos = new GisPos(dataAdapterReader.GetDoubleValue("longitude", 0), dataAdapterReader.GetDoubleValue("latitude", 0));

			if (gisPos.getWgLon() > 180 || gisPos.getWgLon() < 0)
			{
				longitude = 0;
			}
			if (gisPos.getWgLat() > 90 || gisPos.getWgLat() < 0)
			{
				latitude = 0;
			}

			longitude = (int) (gisPos.getWgLon() * 10000000D);
			latitude = (int) (gisPos.getWgLat() * 10000000D);
		}
		catch (Exception e)
		{
			throw e;
		}

		try
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{
				location = 4;
				radius = 80;
				loctp = "wf";
			}
			else
			{
				location = dataAdapterReader.GetIntValue("location", 0);
				radius = dataAdapterReader.GetDoubleValue("radius", 0).intValue();
				loctp = dataAdapterReader.GetStrValue("loctp", "");
			}
			dist = -1000000;
			indoor = dataAdapterReader.GetIntValue("indoor", 0);

			lable = "unknow";

			latlng_time = stime;

			strTime = dataAdapterReader.GetStrValue("Procedure_End_Time", "");
			wifiName = dataAdapterReader.GetStrValue("wifi", "");
			if (strTime.indexOf(".") < 0)
			{
				strTime += ".000000";
			}
			d_beginTime = format.parse(strTime.substring(0, strTime.length() - 3));
			etime = (int) (d_beginTime.getTime() / 1000L);

			Online_ID = 0;
			Session_ID = 0;

			MME_GROUP_ID = 0;
			MME_CODE = 0;
			MME_UE_S1AP_ID = 0;
			ENB_UE_S1AP_ID = 0;
			ENB = "";
			// TAC = dataAdapterReader.GetIntValue("TAC", 0);
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.MSISDN))
			{
				MSISDN = dataAdapterReader.GetStrValue("MSISDN", "");
			}
			else
			{
				MSISDN = "";
			}
			IMEI = dataAdapterReader.GetStrValue("IMEI", "");
			Brand = "";
			Type = "";
			Event_Type = 0;

			IP_LEN_UL = dataAdapterReader.GetIntValue("UL_Data", 0);
			IP_LEN_DL = dataAdapterReader.GetIntValue("DL_Data", 0);
			IP_LEN = 0;
			IP_Data_Len_UL = IP_LEN_UL;// dataAdapterReader.GetIntValue("UL_Data",
										// 0);
			IP_Data_Len_DL = IP_LEN_DL;// dataAdapterReader.GetIntValue("DL_Data",
										// 0);
			// Count_Packet_UL = dataAdapterReader.GetIntValue("UL_IP_Packet",
			// 0);
			// Count_Packet_DL = dataAdapterReader.GetIntValue("DL_IP_Packet",
			// 0);
			User_IP_LEN_UL = 0;
			User_IP_LEN_DL = 0;
			Duration = dataAdapterReader.GetIntValue("last_http_resp_delay_ms", 0);
			Service_Type = dataAdapterReader.GetIntValue("App_Type", 0);
			Sub_Service_Type = dataAdapterReader.GetIntValue("App_Sub_type", 0);
			Result = dataAdapterReader.GetIntValue("App_Status", 0);
			// Result_DelayFirst =
			// dataAdapterReader.GetIntValue("first_req_first_resp_delay_ms",
			// 0);
			// Last_Ack_Delay =
			// dataAdapterReader.GetIntValue("last_ack_delay_ms", 0);
			// Referer =
			// dataAdapterReader.GetStrValue("last_http_resp_delay_ms", "");
			// Retran_Packet_DL =
			// dataAdapterReader.GetIntValue("retran_packet_dl", 0);
			host = dataAdapterReader.GetStrValue("HOST", "");

			Retran_Packet_UL = 0;
			Weight_Rate_UL = 0;
			Weight_Rate_DL = 0;
			Weight_Rate_Total = 0;
		}
		catch (Exception e)
		{

		}

		return true;
	}

	@Override
	public String GetCellKey()
	{
		return String.valueOf(Eci);
	}

	@Override
	public int GetSampleDistance(int ilongitude, int ilatitude)
	{
		LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(Eci);
		if (lteCellInfo != null)
		{
			if (longitude > 0 && latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
			{
				return (int) GisFunction.GetDistance(ilongitude, ilatitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
			}
		}

		return StaticConfig.Int_Abnormal;
	}

	@Override
	public int GetMaxCellRadius()
	{
		int maxRadius = 6000;
		LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(Eci);
		if (lteCellInfo != null)
		{
			maxRadius = Math.min(maxRadius, 5 * lteCellInfo.radius);
			maxRadius = Math.max(maxRadius, 1500);
		}
		return maxRadius;
	}

	public String getEncryptMSISDN()
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EncryptUser))
		{
			return StringUtil.EncryptStringToLong(MSISDN + "") + "";
		}
		return MSISDN;
	}

}
