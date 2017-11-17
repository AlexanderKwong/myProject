package mro.evt;

import xdr.locallex.EventDataStruct;

public abstract class EventData
{
	public int iCityID;
    public long IMSI;
    public long iEci;
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
    
	public EventDataStruct eventDetial;
	
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
		
		eventDetial = new EventDataStruct();
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
}
