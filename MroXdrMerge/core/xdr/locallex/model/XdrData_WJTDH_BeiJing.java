package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import StructData.StaticConfig;
import jan.util.DataAdapterReader;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;

public class XdrData_WJTDH_BeiJing extends XdrDataBase
{
	private Date tmDate = new Date();

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	protected static ParseItem parseItem;
	DataAdapterReader dataAdapterReader;
	protected String strTemp;

	private String 标识;
	private long 分析数据唯一标示;
	private String 小区;
	private int 第一拆线原因;
	private int 释放原因码;
	private int 第一拆线网元类型;
	private int 拆线方向;
	private int TAC终端码;
	private long MME分配的第一个S1APID;
	private long MME分配的最后S1APID;

	public XdrData_WJTDH_BeiJing()
	{
		super();

		if (parseItem == null)
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("ERR-WJTDH");

		dataAdapterReader = new DataAdapterReader(parseItem);
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		imsi = dataAdapterReader.GetLongValue("IMSI", -1);

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		// base property
		tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
		istime = (int) (tmDate.getTime() / 1000L);
		istimems = (int) (tmDate.getTime() % 1000L);
		ietime = istime;
		ietimems = istimems;
		imsi = dataAdapterReader.GetLongValue("IMSI", -1);
		// other property
		标识 = dataAdapterReader.GetStrValue("标识", "");
		分析数据唯一标示 = dataAdapterReader.GetLongValue("分析数据唯一标示", -1);
		strTemp = dataAdapterReader.GetStrValue("小区", "");
		if (strTemp.length() == 12)
		{
			ecgi = Integer.parseInt(strTemp.substring(5, 10), 16) * 256
					+ Integer.parseInt(strTemp.substring(10, 12), 16);
		}
		第一拆线原因 = dataAdapterReader.GetIntValue("第一拆线原因", -1);
		释放原因码 = dataAdapterReader.GetIntValue("释放原因码", -1);
		第一拆线网元类型 = dataAdapterReader.GetIntValue("第一拆线网元类型", -1);
		拆线方向 = dataAdapterReader.GetIntValue("拆线方向", -1);
		TAC终端码 = dataAdapterReader.GetIntValue("TAC终端码", -1);
		MME分配的第一个S1APID = dataAdapterReader.GetLongValue("MME分配的第一个S1APID", -1);
		MME分配的最后S1APID = dataAdapterReader.GetLongValue("MME分配的最后S1APID", -1);
		
		s1apid = MME分配的最后S1APID;

		return true;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();

		EventData eventData = new EventData();
		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = (int)ecgi;
		eventData.iTime = istime;
		eventData.wTimems = 0;
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.Interface = StaticConfig.INTERFACE_WJTDH_BEIJING;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		// event detail
		eventData.eventDetial = new EventDataStruct();
		eventData.eventDetial.strvalue[0] = 标识;
		eventData.eventDetial.fvalue[0] = LteScRSRP;
		eventData.eventDetial.fvalue[1] = LteScSinrUL;
		eventData.eventDetial.fvalue[2] = 分析数据唯一标示;
		eventData.eventDetial.fvalue[3] = (int)ecgi;
		eventData.eventDetial.fvalue[4] = 第一拆线原因;
		eventData.eventDetial.fvalue[5] = 释放原因码;
		eventData.eventDetial.fvalue[6] = 第一拆线网元类型;
		eventData.eventDetial.fvalue[7] = 拆线方向;
		eventData.eventDetial.fvalue[8] = TAC终端码;
		eventData.eventDetial.fvalue[9] = MME分配的第一个S1APID;
		eventData.eventDetial.fvalue[10] = MME分配的最后S1APID;
		
		// event stat
		eventData.eventStat = null;//new EventDataStruct();

		// my data
		eventDataList.add(eventData);

		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		// TODO Auto-generated method stub

	}

}
