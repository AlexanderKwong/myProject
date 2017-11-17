package xdr.lablefill.by23g;

import util.DataGeter;

public class LocInfoItem
{	
	public long session_id;
	public long online_id;
	public int event_type;
	public long imsi;
	public double longitude;
	public double latitude;
	public int location;
	public int dist;
	public int radius;
	public String loctp;
	public long indoor;
	public int networktype;
	public String lac_ci;
	public String uenettype;
	public String timeStr;
	public int lockNetMark;
	public String locTimeStr;

	public boolean FillData(String[] vals, int startPos)
	{
		int i = startPos;
		session_id = DataGeter.GetLong(vals[i++]);
		online_id = DataGeter.GetLong(vals[i++]);
		event_type = DataGeter.GetInt(vals[i++]);
		imsi = DataGeter.GetLong(vals[i++]);
		
		longitude = DataGeter.GetDouble(vals[i++]);
		latitude = DataGeter.GetDouble(vals[i++]);
		
		location = DataGeter.GetInt(vals[i++]);
		dist = DataGeter.GetInt(vals[i++]);
		radius = DataGeter.GetInt(vals[i++]);
		loctp = vals[i++];
		loctp = loctp.equals("NULL")?"":loctp;
		indoor = DataGeter.GetLong(vals[i++]);
		
		networktype = DataGeter.GetInt(vals[i++]);
		lac_ci = vals[i++];
		uenettype = vals[i++];
		timeStr = vals[i++];
		lockNetMark = vals[i].length() == 1 ? DataGeter.GetInt(vals[i]) : 0;
		i++;
		
		locTimeStr = vals[i++];

		return true;
	}
	
	
}
