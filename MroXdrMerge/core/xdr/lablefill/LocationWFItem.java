package xdr.lablefill;

import util.DataGeter;

public class LocationWFItem
{
    public long imsi;
    public String msisdn;
    public int stime;
    public int stime_ms;
    public int etime;
    public int etime_ms;
    public int longitude;
    public int latitude;
    
    private long tmTime;
	
	public LocationWFItem()
	{
		msisdn = "";
	}
	
	public boolean FillData(String[] vals, int startPos)
	{
		int i = startPos;
		
	    try
		{
			imsi =  DataGeter.GetLong(vals[i++], 0);  
			msisdn = DataGeter.GetString(vals[i++], "");  
			tmTime = DataGeter.GetLong(vals[i++], 0); 
			stime = (int)(tmTime/1000L);
			stime_ms = (int)(tmTime%1000L);
			
			tmTime = DataGeter.GetLong(vals[i++], 0); 
			etime = (int)(tmTime/1000L);
			etime_ms = (int)(tmTime%1000L);
			
			longitude = (int)(DataGeter.GetDouble(vals[i++], 0) * 10000000);
			latitude = (int)(DataGeter.GetDouble(vals[i++], 0) * 10000000);
		}
	    catch (Exception e)
	    {
	    	return false;
	    }
		finally
		{
		}
		
		return true;
	}
	
}
