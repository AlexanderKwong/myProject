package xdr.locallex;

import StructData.GridItemOfSize;

public class EventData
{
    public int iCityID;
    public long IMSI;
    public int iEci;
    public int iTime;
    public int wTimems;
    public String strLoctp;
    public String strLabel;
    public int iLongitude;
    public int iLatitude;
    public int iBuildID;
    public int iHeight;
    public int Interface;
    public int iKpiSet;
    public int iProcedureType;
    
    public int iTestType;
    public int iDoorType;
    public int iLocSource;
	
	public EventDataStruct eventDetial;
	public EventDataStruct eventStat;
	
	public GridItemOfSize gridItem;
	public int confidentType;
	
	public int iAreaType;
	public int iAreaID;
	
	public EventData()
	{
		iCityID = -1;
		iTime = 0;
		wTimems = 0;
		strLoctp = "";
		strLabel = "";
		iLongitude = 0;
		iLatitude = 0;
		iBuildID = -1;
		iHeight = -1;
		IMSI = -1;
		iEci = -1;
		Interface = -1;
		iKpiSet = -1; 
		iProcedureType = -1;  
		
		iTestType = -1;
		iDoorType = -1;
		iLocSource = -1;
		
		eventDetial = new EventDataStruct();
		eventStat = new EventDataStruct();
		
		confidentType =-1;
	}
	
	public int toString(StringBuffer sb)
	{
		try
		{
			if(eventDetial == null)
			{
				return -1;
			}			
			
			sb.append(iCityID);sb.append("\t");
			sb.append(IMSI);sb.append("\t");
			sb.append(iEci);sb.append("\t");
			sb.append(wTimems);sb.append("\t");
			sb.append(strLoctp);sb.append("\t");
			sb.append(strLabel);sb.append("\t");
			sb.append(iLongitude);sb.append("\t");
			sb.append(iLatitude);sb.append("\t");
			sb.append(iBuildID);sb.append("\t");
			sb.append(iHeight);sb.append("\t");
			sb.append(Interface);sb.append("\t");
			sb.append(iKpiSet);sb.append("\t");
			// qianmian 13
			sb.append(iProcedureType);sb.append("\t");
			sb.append(iTime);sb.append("\t");
			eventDetial.toString(sb,1);
		}
		catch (Exception e)
		{
			return -1;
		}
        return 0;
	}
	public boolean checkHaveData() 
	{
		boolean flag = false;
		for (int i = 0; i < eventStat.fvalue.length; i++)
		{
			if(eventStat.fvalue[i]>=0){
				flag = true;
			}
		}
		return flag;
	}
	
}
