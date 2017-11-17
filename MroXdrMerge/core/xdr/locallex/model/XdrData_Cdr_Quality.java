package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import jan.util.DataAdapterReader;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;

public class XdrData_Cdr_Quality extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;

	private String LAST_EGCI;
	private long LAST_MME_UE_S1AP_ID;

	private int INTERFACE;
	private double UL_MOS_AVG;
	private long RTCP_UL_PACKET_NUM;
	private long RTCP_UL_LOSSPACKER_NUM;
	private double DL_MOS_AVG;
	private double RTCP_DL_PACKET_NUM;
	private double UL_IPMOS_AVG;
	private double RTP_UL_PACKET_NUM;
	private double RTP_UL_LOSSPACKET_NUM;
	private double DL_IPMOS_AVG;
	private double RTP_DL_PACKET_NUM;
	private double RTP_DL_LOSSPACKET_NUM;
	private double conn_latency;
	private String Service_Type;

	// 统计指标
	private double volte上行rtcp总包数;
	private double volte上行rtcp丢包数;
	private double volte下行rtcp总包数;
	private double VOLTE下行RTP丢包数_GM;
	private double VOLTE下行RTP总包数_GM;
	private double VoLTE下行RTP丢包数_S1U;
	private double VOLTE下行RTP总包数_S1U;
	private double VoLTE下行RTP丢包数_MW;
	private double VOLTE下行RTP总包数_MW;
	private double VoLTE上行RTP丢包数_S1U;
	private double VoLTE上行RTP总包数_S1U;
	private double VoLTE上行RTP丢包数_GM;
	private double VoLTE上行RTP总包数_GM;
	private double VoLTE上行RTP丢包数_MW;
	private double VoLTE上行RTP总包数_MW;

	public XdrData_Cdr_Quality()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-CDR-QUALITY");
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

			s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);

			try
			{
				String strEci = dataAdapterReader.GetStrValue("LAST_EGCI", "");
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

			// stime
			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = dataAdapterReader.GetIntValue("STARTTIME_MS", 0);

			// etime
			tmDate = dataAdapterReader.GetDateValue("ENDTIME", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = dataAdapterReader.GetIntValue("ENDTIME_MS", 0);

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
			s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", StaticConfig.Int_Abnormal);

			try
			{
				String strEci = dataAdapterReader.GetStrValue("LAST_EGCI", "");
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

			// stime
			tmDate = dataAdapterReader.GetDateValue("STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = dataAdapterReader.GetIntValue("STARTTIME_MS", 0);

			// etime
			tmDate = dataAdapterReader.GetDateValue("ENDTIME", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = dataAdapterReader.GetIntValue("ENDTIME_MS", 0);

			INTERFACE = dataAdapterReader.GetIntValue("INTERFACE", StaticConfig.Int_Abnormal);
			UL_MOS_AVG = dataAdapterReader.GetDoubleValue("UL_MOS_AVG", StaticConfig.Int_Abnormal);
			RTCP_UL_PACKET_NUM = dataAdapterReader.GetLongValue("RTCP_UL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTCP_UL_LOSSPACKER_NUM = dataAdapterReader.GetLongValue("RTCP_UL_LOSSPACKER_NUM",
					StaticConfig.Int_Abnormal);
			DL_MOS_AVG = dataAdapterReader.GetDoubleValue("DL_MOS_AVG", StaticConfig.Int_Abnormal);
			RTCP_DL_PACKET_NUM = dataAdapterReader.GetDoubleValue("RTCP_DL_PACKET_NUM", StaticConfig.Int_Abnormal);
			UL_IPMOS_AVG = dataAdapterReader.GetDoubleValue("UL_IPMOS_AVG", StaticConfig.Int_Abnormal);
			RTP_UL_PACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_UL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTP_UL_LOSSPACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_UL_LOSSPACKET_NUM",
					StaticConfig.Int_Abnormal);
			DL_IPMOS_AVG = dataAdapterReader.GetDoubleValue("DL_IPMOS_AVG", StaticConfig.Int_Abnormal);
			RTP_DL_PACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_DL_PACKET_NUM", StaticConfig.Int_Abnormal);
			RTP_DL_LOSSPACKET_NUM = dataAdapterReader.GetDoubleValue("RTP_DL_LOSSPACKET_NUM",
					StaticConfig.Int_Abnormal);
			conn_latency = dataAdapterReader.GetDoubleValue("conn_latency", StaticConfig.Int_Abnormal);

			Service_Type = dataAdapterReader.GetStrValue("Service_Type", null);

			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}

	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		/**
		 * TODO Service_Type
		 */
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		if (conn_latency > 0)
		{ // &&SERVICE_TYPE==1
			if (INTERFACE == 34 && UL_MOS_AVG > 0)
			{
				volte上行rtcp总包数 = RTCP_UL_PACKET_NUM;
			}

			if (INTERFACE == 34 && UL_MOS_AVG > 0)
			{
				volte上行rtcp丢包数 = RTCP_UL_LOSSPACKER_NUM;
			}
			if (INTERFACE == 34 && DL_MOS_AVG > 0)
			{
				volte下行rtcp总包数 = RTCP_DL_PACKET_NUM;
			}

			if (INTERFACE == 54 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP丢包数_GM = RTP_DL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 54 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP总包数_GM = RTP_DL_PACKET_NUM;
			}

			if (INTERFACE == 34 && DL_IPMOS_AVG > 0)
			{
				VoLTE下行RTP丢包数_S1U = RTP_DL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 34 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP总包数_S1U = RTP_DL_PACKET_NUM;
			}

			if (INTERFACE == 55 && DL_IPMOS_AVG > 0)
			{
				VoLTE下行RTP丢包数_MW = RTP_DL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 55 && DL_IPMOS_AVG > 0)
			{
				VOLTE下行RTP总包数_MW = RTP_DL_PACKET_NUM;
			}

			if (INTERFACE == 34 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP丢包数_S1U = RTP_UL_LOSSPACKET_NUM;
			}

			if (INTERFACE == 34 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP总包数_S1U = RTP_UL_PACKET_NUM;
			}

			if (INTERFACE == 54 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP丢包数_GM = RTP_UL_LOSSPACKET_NUM;
			}
			if (INTERFACE == 54 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP总包数_GM = RTP_UL_PACKET_NUM;
			}

			if (INTERFACE == 55 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP丢包数_MW = RTP_UL_LOSSPACKET_NUM;
			}
			if (INTERFACE == 55 && UL_IPMOS_AVG > 0)
			{
				VoLTE上行RTP总包数_MW = RTP_UL_PACKET_NUM;
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
			eventData.iKpiSet = 2;
			eventData.iProcedureType = 1;

			eventData.iTestType = testType;
			eventData.iDoorType = iDoorType;
			eventData.iLocSource = locSource;

			eventData.confidentType = confidentType;
			eventData.iAreaType = iAreaType;
			eventData.iAreaID = iAreaID;

			// event stat
			eventData.eventStat = new EventDataStruct();

			eventData.eventStat.fvalue[0] = volte上行rtcp总包数;
			eventData.eventStat.fvalue[1] = volte上行rtcp丢包数;
			eventData.eventStat.fvalue[2] = volte下行rtcp总包数;
			eventData.eventStat.fvalue[3] = VOLTE下行RTP丢包数_GM;
			eventData.eventStat.fvalue[4] = VOLTE下行RTP总包数_GM;
			eventData.eventStat.fvalue[5] = VoLTE下行RTP丢包数_S1U;
			eventData.eventStat.fvalue[6] = VOLTE下行RTP总包数_S1U;
			eventData.eventStat.fvalue[7] = VoLTE下行RTP丢包数_MW;
			eventData.eventStat.fvalue[8] = VOLTE下行RTP总包数_MW;
			eventData.eventStat.fvalue[9] = VoLTE上行RTP丢包数_S1U;
			eventData.eventStat.fvalue[10] = VoLTE上行RTP总包数_S1U;
			eventData.eventStat.fvalue[11] = VoLTE上行RTP丢包数_GM;
			eventData.eventStat.fvalue[12] = VoLTE上行RTP总包数_GM;
			eventData.eventStat.fvalue[13] = VoLTE上行RTP丢包数_MW;
			eventData.eventStat.fvalue[14] = VoLTE上行RTP总包数_MW;
			
			eventData.eventDetial = null;
			eventDataList.add(eventData);

		}

		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		// TODO Auto-generated method stub

	}

}
