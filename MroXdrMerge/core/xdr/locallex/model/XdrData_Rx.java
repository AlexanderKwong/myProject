package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;
import jan.util.DataAdapterReader;

public class XdrData_Rx extends XdrDataBase
{
	private Date tmDate = new Date();
	
	private static ParseItem parseItem;
	public int Interface;
	public String IMEI;
	public int ProceDure_Type;

	public int ABORT_CAUSE;
	public int MEDIA_TYPE;
	public int RESULT_CODE;

	public int ECI;

	// 统计字段
	private long VoLTE语音掉话次数 = StaticConfig.Int_Abnormal;
	private long VoLTE视频掉话次数 = StaticConfig.Int_Abnormal;

	public XdrData_Rx()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Rx");
		}
	}

	public void clear()
	{
		Interface = StaticConfig.Int_Abnormal;
		IMEI = "";
		ProceDure_Type = StaticConfig.Int_Abnormal;

		ABORT_CAUSE = StaticConfig.Int_Abnormal;
		MEDIA_TYPE = 0; // StaticConfig.Int_Abnormal; 因为这个原始数据中为空
		RESULT_CODE = StaticConfig.Int_Abnormal;
		ECI = StaticConfig.Int_Abnormal;
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
		// public int IMSI;
		IMEI = dataAdapterReader.GetStrValue("IMEI", "");
		ProceDure_Type = dataAdapterReader.GetIntValue("ProceDure_Type", StaticConfig.Int_Abnormal);

		ABORT_CAUSE = dataAdapterReader.GetIntValue("ABORT_CAUSE", StaticConfig.Int_Abnormal);
		MEDIA_TYPE = dataAdapterReader.GetIntValue("MEDIA_TYPE", 0);
		RESULT_CODE = dataAdapterReader.GetIntValue("RESULT_CODE", StaticConfig.Int_Abnormal);

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
		boolean haveEventData = false;
		if (ProceDure_Type == 4 && ABORT_CAUSE != 3 && MEDIA_TYPE == 0)
		{
			VoLTE语音掉话次数 = 1;
			haveEventData=true;
		}
		if (ProceDure_Type == 4 && ABORT_CAUSE != 3 && MEDIA_TYPE == 1)
		{
			VoLTE视频掉话次数 = 1;
			haveEventData=true;
		}
		if(!haveEventData){
			return null;
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
		eventData.Interface = StaticConfig.INTERFACE_RX; 
		eventData.iKpiSet = 1;
		eventData.iProcedureType = ProceDure_Type;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType =confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		
		
		eventData.eventStat.fvalue[0] = VoLTE语音掉话次数;
		eventData.eventStat.fvalue[1] = VoLTE视频掉话次数;
		
		
		eventData.eventDetial.strvalue[0]="掉话";
		eventData.eventDetial.fvalue[0] = LteScRSRP;
		eventData.eventDetial.fvalue[1] = LteScSinrUL;
		eventData.eventDetial.fvalue[2] = ABORT_CAUSE;
		eventData.eventDetial.fvalue[3] = MEDIA_TYPE;
		eventData.eventDetial.fvalue[4] = RESULT_CODE;
		
		eventDataList.add(eventData);
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		

	}

}
