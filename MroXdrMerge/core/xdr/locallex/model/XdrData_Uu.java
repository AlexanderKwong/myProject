package xdr.locallex.model;

import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.math3.ode.events.EventState;

import StructData.GridItem;
import StructData.StaticConfig;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;

public class XdrData_Uu extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	private StringBuffer value;

	public int Procedure_Type;
	public int Procedure_Status;
	public int KeyWord1;
	public int tac;

	private long eNB间切换出成功次数;
	private long eNB间切换出请求次数;
	private long eNB内切换出成功次数;
	private long eNB内切换出请求次数;
	private long RRC连接建立成功次数;
	private long RRC连接建立请求次数;
	private long RRC连接建立时长;
	private long RRC连接重建成功次数;

	public static HashMap<Integer, Integer> produceTypeMaps = new HashMap<>();

	public XdrData_Uu()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-uu");
		}
	}

	public void clear()
	{
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
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);
			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

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
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			Procedure_Type = dataAdapterReader.GetIntValue("Procedure_Type", StaticConfig.Int_Abnormal);
			Procedure_Status = dataAdapterReader.GetIntValue("Procedure_Status", StaticConfig.Int_Abnormal);
			KeyWord1 = dataAdapterReader.GetIntValue("KeyWord1", StaticConfig.Int_Abnormal);
			tac = dataAdapterReader.GetIntValue("Tac", StaticConfig.Int_Abnormal);
			// TODO 看地市了
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
			{
				initUUProduceType();

				ecgi = dataAdapterReader.GetLongValue("CELLID", 0);
				s1apid = dataAdapterReader.GetLongValue("LAST_MME_UE_S1AP_ID", 0);
				if (produceTypeMaps.containsKey(Procedure_Type))
				{
					Procedure_Type = produceTypeMaps.get(Procedure_Type);
				}
			}
			else
			{
				ecgi = dataAdapterReader.GetLongValue("ENBID", 0) * 256 + dataAdapterReader.GetLongValue("CELLID", 0);
			}

			// value = dataAdapterReader.getTmStrs();
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
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataLists = new ArrayList<EventData>();

		boolean haveEventStat = false;
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)){
			haveEventStat = statNingXiaToEvent(haveEventStat);
		}else{
			haveEventStat = statToEvent(haveEventStat);
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
		eventData.Interface = StaticConfig.INTERFACE_UU;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;

		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		eventData.eventStat.fvalue[0] = eNB间切换出成功次数;
		eventData.eventStat.fvalue[1] = eNB间切换出请求次数;
		eventData.eventStat.fvalue[2] = eNB内切换出成功次数;
		eventData.eventStat.fvalue[3] = eNB内切换出请求次数;
		eventData.eventStat.fvalue[4] = RRC连接建立成功次数;
		eventData.eventStat.fvalue[5] = RRC连接建立请求次数;
		eventData.eventStat.fvalue[6] = RRC连接建立时长;
		eventData.eventStat.fvalue[7] = RRC连接重建成功次数;

		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		if (haveEventStat)
		{

			// TODO 先暂时把eventDetail设置成null
			eventData.eventDetial = null;
			eventDataLists.add(eventData);
		}

		// 不用写了
		return eventDataLists;
	}

	private boolean statNingXiaToEvent(boolean haveEventStat)
	{
		if (Procedure_Type == 8 && Procedure_Status == 1)
		{
			eNB间切换出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 8)
		{
			eNB间切换出请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 7 && Procedure_Status == 1)
		{
			eNB内切换出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 7)
		{
			eNB内切换出请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 1 && Procedure_Status == 1)
		{
			RRC连接建立成功次数 = 1;
			RRC连接建立时长 = ietime * 1000L + ietimems - istime * 1000L - istimems;
			haveEventStat = true;
		}
		if (Procedure_Type == 1)
		{
			RRC连接建立请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 4 && Procedure_Status == 1)
		{
			RRC连接重建成功次数 = 1;
			haveEventStat = true;
		}
		return haveEventStat;
	}

	private boolean statToEvent(boolean haveEventStat)
	{
		if (Procedure_Type == 8 && Procedure_Status == 0)
		{
			eNB间切换出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 8)
		{
			eNB间切换出请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 7 && Procedure_Status == 0)
		{
			eNB内切换出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 7)
		{
			eNB内切换出请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 1 && Procedure_Status == 0)
		{
			RRC连接建立成功次数 = 1;
			RRC连接建立时长 = ietime * 1000L + ietimems - istime * 1000L - istimems;
			haveEventStat = true;
		}
		if (Procedure_Type == 1)
		{
			RRC连接建立请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 4 && Procedure_Status == 0)
		{
			RRC连接重建成功次数 = 1;
			haveEventStat = true;
		}
		return haveEventStat;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if (fenge.contains("\\"))
		{
			fenge = fenge.replace("\\", "");
		}

		sb.append(value);
		sb.append(fenge);
		sb.append(iLongitude);
		sb.append(fenge);
		sb.append(iLatitude);
		sb.append(fenge);
		sb.append(iheight);
		sb.append(fenge);
		sb.append(iDoorType);
		sb.append(fenge);

		sb.append(iRadius);
		sb.append(fenge);
		GridItem gridItem = GridItem.GetGridItem(0, iLongitude, iLatitude);

		int icentLng = gridItem.getBRLongitude() / 2 + gridItem.getTLLongitude() / 2;
		int icentLat = gridItem.getBRLatitude() / 2 + gridItem.getTLLatitude() / 2;

		if (StaticConfig.cityId_Name.containsKey(iCityID))
		{
			sb.append(StaticConfig.cityId_Name.get(iCityID) + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}
		else
		{
			sb.append("nocity" + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}

		sb.append(-1);
		sb.append(fenge);
		sb.append(-1);
	}

	private void initUUProduceType()
	{
		if (produceTypeMaps.size() == 0)
		{
			produceTypeMaps.put(200, 1);
			produceTypeMaps.put(201, 4);
			produceTypeMaps.put(202, 3);
			produceTypeMaps.put(203, 2);
			produceTypeMaps.put(204, 5);
			produceTypeMaps.put(205, 6);
			produceTypeMaps.put(206, 7);
			produceTypeMaps.put(207, 8);
			produceTypeMaps.put(208, 9);
			produceTypeMaps.put(209, 10);
			produceTypeMaps.put(210, 11);
			produceTypeMaps.put(211, 12);
			produceTypeMaps.put(212, 13);
		}
	}

}
