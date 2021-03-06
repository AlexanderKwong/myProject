package xdr.locallex.model;

import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import StructData.GridItem;
import StructData.StaticConfig;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;

public class XdrData_Rtp extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;

	private StringBuffer value; 
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public XdrData_Rtp()
	{
		super();
		clear();
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if(parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-S1-Rtp");
		}		
	}
	
	public void clear(){
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
			// stime 2017-08-30 11:43:53.142244815
			String startTimeStr=dataAdapterReader.GetStrValue("Procedure_Start_Time", ""); 
			startTimeStr = startTimeStr.substring(0,19);
			tmDate = dateFormat.parse(startTimeStr);
			istime = (int) (tmDate.getTime()/1000);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			String endtimeStr=dataAdapterReader.GetStrValue("Procedure_End_Time", ""); 
			endtimeStr = endtimeStr.substring(0, 19);
			tmDate = dateFormat.parse(endtimeStr);
			ietime = (int) (tmDate.getTime()/1000);
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
			// stime 2017-08-30 11:43:53.142244815
			String startTimeStr=dataAdapterReader.GetStrValue("Procedure_Start_Time", ""); 
			startTimeStr = startTimeStr.substring(0,19);
			tmDate = dateFormat.parse(startTimeStr);
			istime = (int) (tmDate.getTime()/1000);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			String endtimeStr=dataAdapterReader.GetStrValue("Procedure_End_Time", ""); 
			endtimeStr = endtimeStr.substring(0, 19);
			tmDate = dateFormat.parse(endtimeStr);
			ietime = (int) (tmDate.getTime()/1000);
			ietimems = (int) (tmDate.getTime() % 1000L);

			value = dataAdapterReader.getTmStrs();
		}
		catch (Exception e)
		{
			return false;
		}
		
		return true;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		// 不用写了
		return new ArrayList<>();
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
