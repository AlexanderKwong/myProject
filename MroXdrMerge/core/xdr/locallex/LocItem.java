package xdr.locallex;

import util.DataGeter;

public class LocItem
{
	public int cityID;
	public int itime;
	public short wtimems;
	public long IMSI;
	public int ilongitude;
	public int ilatitude;
	public int ibuildid;
	public int iheight;
	public int testType;
	public int doorType;
	public int radius;
	public String loctp;
	public String label;
	public int iAreaType;
	public int iAreaID;
	public int locSource;
	public int LteScRSRP;
	public int LteScSinrUL;
	public long eci;
	public int confidentType;
	//yzx add 2017.10.24 
	public long s1apid;
	public String msisdn;

	public LocItem()
	{
		cityID = -1;
		itime = -1;
		wtimems = -1;
		IMSI = -1;
		ilongitude = -1;
		ilatitude = -1;
		ibuildid = -1;
		iheight = -1;
		testType = -1;
		doorType = -1;
		radius = -1;
		loctp = "";
		label = "";
		iAreaType = -1;
		iAreaID = -1;
		locSource = -1;
		LteScRSRP = -1000000;
		LteScSinrUL = -1000000;
		eci = 0;
		confidentType = 0;
		msisdn = "";
		s1apid = 0;
	}

	public static LocItem filleData(String[] vals)
	{
		LocItem loc = new LocItem();
		int i = 0;
		loc.cityID = Integer.parseInt(vals[i++]);
		loc.itime = Integer.parseInt(vals[i++]);
		loc.wtimems = Short.parseShort(vals[i++]);
		loc.IMSI = Long.parseLong(vals[i++]);
		loc.ilongitude = Integer.parseInt(vals[i++]);
		loc.ilatitude = Integer.parseInt(vals[i++]);
		loc.ibuildid = Integer.parseInt(vals[i++]);
		loc.iheight = Integer.parseInt(vals[i++]);
		loc.testType = Integer.parseInt(vals[i++]);

		loc.doorType = DataGeter.GetInt(vals[i++], -1);

		loc.radius = Integer.parseInt(vals[i++]);
		loc.loctp = vals[i++];
		loc.label = vals[i++];
		try
		{
			if (i < vals.length)
			{
				loc.iAreaType = Integer.parseInt(vals[i++]);
			}
			if (i < vals.length)
			{
				loc.iAreaID = Integer.parseInt(vals[i++]);
			}

			if (i < vals.length)
			{
				loc.locSource = Integer.parseInt(vals[i++]);
			}
			if (i < vals.length)
			{
				loc.LteScRSRP = DataGeter.GetInt(vals[i++], -1000000);
			}
			if (i < vals.length)
			{
				loc.LteScSinrUL = DataGeter.GetInt(vals[i++], -1000000);
			}
			if (i < vals.length)
			{
				loc.eci = DataGeter.GetLong(vals[i++], 0);
			}
			if (i < vals.length)
			{
				loc.confidentType = DataGeter.GetInt(vals[i++], 0);
			}
			if (i < vals.length)
			{
				loc.msisdn = vals[i++];
			}
			if (i < vals.length)
			{
				loc.s1apid = DataGeter.GetLong(vals[i++], 0);
			}

		}
		catch (Exception e)
		{
			// 什么都不用做,try catch只是为了前面能正常运行
		}

		return loc;
	}

	private StringBuffer sb = new StringBuffer();

	public String toString()
	{
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append("\t");
		sb.append(itime);
		sb.append("\t");
		sb.append(wtimems);
		sb.append("\t");
		sb.append(IMSI);
		sb.append("\t");
		sb.append(ilongitude);
		sb.append("\t");
		sb.append(ilatitude);
		sb.append("\t");
		sb.append(ibuildid);
		sb.append("\t");
		sb.append(iheight);
		sb.append("\t");
		sb.append(testType);
		sb.append("\t");
		sb.append(doorType);
		sb.append("\t");
		sb.append(radius);
		sb.append("\t");
		sb.append(loctp);
		sb.append("\t");
		sb.append(label);
		sb.append("\t");
		sb.append(iAreaType);
		sb.append("\t");
		sb.append(iAreaID);
		sb.append("\t");
		sb.append(locSource);
		sb.append("\t");
		sb.append(LteScRSRP);
		sb.append("\t");
		sb.append(LteScSinrUL);
		sb.append("\t");
		sb.append(eci);
		sb.append("\t");
		sb.append(confidentType);
		sb.append("\t");
		sb.append(msisdn);
		sb.append("\t");
		sb.append(s1apid);
		

		return sb.toString();
	}

}
