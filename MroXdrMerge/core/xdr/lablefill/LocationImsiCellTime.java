package xdr.lablefill;

public class LocationImsiCellTime
{
	public long Imsi;
	public long dayhour;
	public long eci;
	public long times;
	public long longtitude;
	public long latitude;
	public int num;
	public String msisdn;
	public int amOrPm;// 0：白天1：夜晚

	public LocationImsiCellTime(long imsi, long eci, long dayhour, long times, String msisdn)
	{
		this.Imsi = imsi;
		this.dayhour = dayhour;
		this.eci = eci;
		this.times = times;
		this.msisdn = msisdn;
	}

	public LocationImsiCellTime(String args[])
	{
		int i = 0;
		this.Imsi = Long.parseLong(args[i++]);
		this.dayhour = Long.parseLong(args[i++]);
		this.eci = Long.parseLong(args[i++]);
		this.times = Long.parseLong(args[i++]);
		this.longtitude = Long.parseLong(args[i++]);
		this.latitude = Long.parseLong(args[i++]);
		this.msisdn = args[i++];
	}

	public String toString()
	{
		long aveLongtitude = 0;
		long aveLatitude = 0;
		if (num > 0)
		{
			aveLongtitude = longtitude / num;
			aveLatitude = latitude / num;
		}
		else
		{
			aveLongtitude = 0;
			aveLatitude = 0;
		}
		return Imsi + "," + dayhour + "," + eci + "," + times + "," + aveLongtitude + "," + aveLatitude + "," + msisdn;
	}

}
