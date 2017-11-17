package xdr.lablefill;

import util.DataGeter;

public class LocationItem
{
	public long imsi;
	public int itime;
	public int itimeMS;
	public int locTime;
	public int locTimeMS;
	public int eci;
	public String userIP = "";
	public int port;
	public String serverIP = "";
	public int location;
	public String loctp = "";
	public int radius;
	public int longitude;
	public int latitude;
	public String wifiName = "";

	public long tmTime = 0;

	public boolean FillData(String[] vals, int startPos)
	{
		int i = startPos;
		try
		{
			imsi = DataGeter.GetLong(vals[i++], 0);
			tmTime = DataGeter.GetLong(vals[i++], 0);
			itime = (int) (tmTime / 1000L);
			itimeMS = (int) (tmTime % 1000L);

			tmTime = DataGeter.GetLong(vals[i++], 0);
			locTime = (int) (tmTime / 1000L);
			locTimeMS = (int) (tmTime % 1000L);
		}
		catch (Exception e)
		{
		}
		finally
		{
			// TODO: handle finally clause
		}

		eci = DataGeter.GetInt(vals[i++]);
		userIP = vals[i++];
		port = DataGeter.GetInt(vals[i++], 0);
		serverIP = vals[i++];
		location = DataGeter.GetInt(vals[i++], 0);
		loctp = vals[i++];
		radius = (int) (DataGeter.GetDouble(vals[i++], -1));
		longitude = (int) (DataGeter.GetDouble(vals[i++], 0) * 10000000);
		latitude = (int) (DataGeter.GetDouble(vals[i++], 0) * 10000000);
		i++;// location bak
		if (i <= vals.length - 1)
		{
			wifiName = vals[i++];
		}

		// 格式化数据
		if (loctp.equals("lll"))
		{
			loctp = "ll";
		}

		if (itime == 0)
		{
			itime = locTime;
		}

		return true;
	}

}
