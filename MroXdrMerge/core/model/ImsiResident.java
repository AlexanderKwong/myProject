package model;

public class ImsiResident
{//imsi，小时（0-23），eci，停留时长，经度，纬度，手机号，白天晚上（0：白天 1：晚上）、buildid、楼宇经度、楼宇纬度
	private long imsi;
	
	private int time;
	
	private long eci;
	
	private int hours;
	
	private int longtitude;
	
	private int lattitude;
	
	private String phoneNum;
	
	private int dayOrNight;
	
	private int buildId;
	
	private int buildLongtitude;
	
	private int buildLattitude;

	public long getImsi()
	{
		return imsi;
	}

	public void setImsi(long imsi)
	{
		this.imsi = imsi;
	}

	public int getTime()
	{
		return time;
	}

	public void setTime(int time)
	{
		this.time = time;
	}

	public long getEci()
	{
		return eci;
	}

	public void setEci(long eci)
	{
		this.eci = eci;
	}

	public int getHours()
	{
		return hours;
	}

	public void setHours(int hours)
	{
		this.hours = hours;
	}

	public int getLongtitude()
	{
		return longtitude;
	}

	public void setLongtitude(int longtitude)
	{
		this.longtitude = longtitude;
	}

	public int getLattitude()
	{
		return lattitude;
	}

	public void setLattitude(int lattitude)
	{
		this.lattitude = lattitude;
	}

	public String getPhoneNum()
	{
		return phoneNum;
	}

	public void setPhoneNum(String phoneNum)
	{
		this.phoneNum = phoneNum;
	}

	public int getDayOrNight()
	{
		return dayOrNight;
	}

	public void setDayOrNight(int dayOrNight)
	{
		this.dayOrNight = dayOrNight;
	}

	public int getBuildId()
	{
		return buildId;
	}

	public void setBuildId(int buildId)
	{
		this.buildId = buildId;
	}

	public int getBuildLongtitude()
	{
		return buildLongtitude;
	}

	public void setBuildLongtitude(int buildLongtitude)
	{
		this.buildLongtitude = buildLongtitude;
	}

	public int getBuildLattitude()
	{
		return buildLattitude;
	}

	public void setBuildLattitude(int buildLattitude)
	{
		this.buildLattitude = buildLattitude;
	}

	public static ImsiResident fillData(String[] strs){
		//imsi，小时（0-23），eci，停留时长，经度，纬度，手机号，白天晚上（0：白天 1：晚上）、buildid、楼宇经度、楼宇纬度
		int i = 0;
		ImsiResident imsiResident = new ImsiResident();
		imsiResident.imsi = Integer.parseInt(strs[i++]);
		imsiResident.time = Integer.parseInt(strs[i++]);
		imsiResident.eci = Long.parseLong(strs[i++]);
		imsiResident.hours = Integer.parseInt(strs[i++]);
		imsiResident.longtitude = Integer.parseInt(strs[i++]);
		imsiResident.lattitude = Integer.parseInt(strs[i++]);
		imsiResident.phoneNum = strs[i++];
		imsiResident.dayOrNight = Integer.parseInt(strs[i++]);
		imsiResident.buildId = Integer.parseInt(strs[i++]);
		imsiResident.buildLongtitude = Integer.parseInt(strs[i++]);
		imsiResident.buildLattitude = Integer.parseInt(strs[i++]);
		return imsiResident;
	}
}
