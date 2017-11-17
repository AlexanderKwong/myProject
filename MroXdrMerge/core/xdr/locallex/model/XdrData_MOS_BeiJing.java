package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;

public class XdrData_MOS_BeiJing extends XdrDataBase
{	
	private Date tmDate = new Date();
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	protected static ParseItem parseItem;	
	DataAdapterReader dataAdapterReader;
	protected String strTemp;
	
		
	private int 用户面流持续时长;
	private String 标识;
	private int 通话时长;
	private int 主被叫标识;
	private int 上行MOS差周期次数;
	private int 下行MOS差周期次数;
	private int 上行IPMOS差周期次数;
	private int 下行IPMOS差周期次数;
	private double 上行ipmos质差占比;
	private double 下行ipmos质差占比;
	private long 分析数据唯一标示;
	private int 终端TAC码;
	private String 小区;
	private String 结束小区信息;
	private long 开始MMES1AP;
	private long 结束MMES1AP;
	
	
	public XdrData_MOS_BeiJing()
	{
		super();
		
		if(parseItem == null)
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("ERR-MOS");
		
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
		//base property
		tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
		istime = (int)(tmDate.getTime()/1000L);
		istimems = (int) (tmDate.getTime() % 1000L);
		ietime = istime;
		ietimems = istimems;
		imsi = dataAdapterReader.GetLongValue("IMSI", -1);
		
		//other property
		用户面流持续时长 = dataAdapterReader.GetIntValue("用户面流持续时长", -1);
		标识 = dataAdapterReader.GetStrValue("标识", "");
		通话时长 = dataAdapterReader.GetIntValue("通话时长", -1);
		主被叫标识 = dataAdapterReader.GetIntValue("主被叫标识", -1);
		上行MOS差周期次数 = dataAdapterReader.GetIntValue("上行MOS差周期次数", -1);
		下行MOS差周期次数 = dataAdapterReader.GetIntValue("下行MOS差周期次数", -1);
		上行IPMOS差周期次数 = dataAdapterReader.GetIntValue("上行IPMOS差周期次数", -1);
		下行IPMOS差周期次数 = dataAdapterReader.GetIntValue("下行IPMOS差周期次数", -1);
		上行ipmos质差占比 = dataAdapterReader.GetDoubleValue("上行ipmos质差占比", -1);
		下行ipmos质差占比 = dataAdapterReader.GetDoubleValue("下行ipmos质差占比", -1);
		分析数据唯一标示 = dataAdapterReader.GetLongValue("分析数据唯一标示", -1);
		try{
			终端TAC码 = dataAdapterReader.GetIntValue("终端TAC码", -1);	
		}catch(Exception e){
			
		}

		小区 = dataAdapterReader.GetStrValue("小区", "");
		strTemp = dataAdapterReader.GetStrValue("结束小区信息", "");
		if(strTemp.length() == 12)
		{
			ecgi = Integer.parseInt(strTemp.substring(5,10), 16) * 256 + Integer.parseInt(strTemp.substring(10,12), 16);
		}	
		开始MMES1AP = dataAdapterReader.GetLongValue("开始MMES1AP", -1);
		结束MMES1AP = dataAdapterReader.GetLongValue("结束MMES1AP", -1);
		s1apid = 结束MMES1AP;
			
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
		eventData.Interface = StaticConfig.INTERFACE_MOS_BEIJING;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;
		
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;	
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		
		//event detail
		eventData.eventDetial = new EventDataStruct();
		eventData.eventDetial.strvalue[0] = 标识;
		eventData.eventDetial.fvalue[0] = LteScRSRP;
		eventData.eventDetial.fvalue[1] = LteScSinrUL;
		eventData.eventDetial.fvalue[2] = 上行MOS差周期次数;
		eventData.eventDetial.fvalue[3] = 下行MOS差周期次数;
		eventData.eventDetial.fvalue[4] = 上行IPMOS差周期次数;
		eventData.eventDetial.fvalue[5] = 下行IPMOS差周期次数;
		eventData.eventDetial.fvalue[6] = 上行ipmos质差占比;
		eventData.eventDetial.fvalue[7] = 下行ipmos质差占比;
		eventData.eventDetial.fvalue[8] = 分析数据唯一标示;
		eventData.eventDetial.fvalue[9] = 终端TAC码;
		eventData.eventDetial.fvalue[10] = 主被叫标识;
		eventData.eventDetial.fvalue[11] = 通话时长;
		eventData.eventDetial.fvalue[12] = 结束MMES1AP;
		
		//event stat
		eventData.eventStat = new EventDataStruct();
		eventData.eventStat.fvalue[0] = 上行MOS差周期次数;
		eventData.eventStat.fvalue[1] = 下行MOS差周期次数;
		eventData.eventStat.fvalue[2] = 上行IPMOS差周期次数;
		eventData.eventStat.fvalue[3] = 下行IPMOS差周期次数;
		if(上行MOS差周期次数<=0 && 下行MOS差周期次数<=0 && 上行IPMOS差周期次数<=0 && 下行IPMOS差周期次数<=0){
			eventData.eventStat = null;
		}	
		eventDataList.add(eventData);
		
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		
	}



    
	
	
	
	
}
