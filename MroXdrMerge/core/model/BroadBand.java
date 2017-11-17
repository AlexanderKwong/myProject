package model;

public class BroadBand
{
	//手机号码 、经度、纬度、楼层、楼宇id
	
	private String phoneNumString;
	
	private int buildLongtitude;
	
	private int buildLattitude;
	
	private int floor;
	
	private int buildId;

	public String getPhoneNumString()
	{
		return phoneNumString;
	}

	public void setPhoneNumString(String phoneNumString)
	{
		this.phoneNumString = phoneNumString;
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

	public int getFloor()
	{
		return floor;
	}

	public void setFloor(int floor)
	{
		this.floor = floor;
	}

	public int getBuildId()
	{
		return buildId;
	}

	public void setBuildId(int buildId)
	{
		this.buildId = buildId;
	}

	public static BroadBand fillData(String[] strs){
		int i = 0;
		BroadBand broadBand = new BroadBand();
		broadBand.phoneNumString = strs[i++];
		broadBand.buildLongtitude = Integer.parseInt(strs[i++]);
		broadBand.buildLattitude = Integer.parseInt(strs[i++]);
		broadBand.floor = Integer.parseInt(strs[i++]);
		broadBand.buildId = Integer.parseInt(strs[i++]);
		return broadBand;
	}
}
