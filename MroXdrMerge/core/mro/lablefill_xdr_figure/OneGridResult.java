package mro.lablefill_xdr_figure;

import java.util.ArrayList;

public class OneGridResult
{
	private ArrayList<Double> oneresult;
	private int level;
	private long ilongitude;
	private long ilatitude;
	private long gongcanIlongitude;
	private long gongcanIlatitud;
	private int buildingId;

	public OneGridResult(ArrayList<Double> oneresult, int level, long ilongitude, long ilatitude,
			long gongcanIlongitude, long gongcanIlatitud, int buildingId)
	{
		this.oneresult = oneresult;
		this.level = level;
		this.ilongitude = ilongitude;
		this.ilatitude = ilatitude;
		this.gongcanIlongitude = gongcanIlongitude;
		this.gongcanIlatitud = gongcanIlatitud;
		this.buildingId = buildingId;
	}

	public ArrayList<Double> getOneresult()
	{
		return oneresult;
	}

	public int getBuildingId()
	{
		return buildingId;
	}

	public void setBuildingId(int buildingId)
	{
		this.buildingId = buildingId;
	}

	public int getLevel()
	{
		return level;
	}

	public long getIlongitude()
	{
		return ilongitude;
	}

	public long getIlatitude()
	{
		return ilatitude;
	}

	public long getGongcanIlongitude()
	{
		return gongcanIlongitude;
	}

	public long getGongcanIlatitud()
	{
		return gongcanIlatitud;
	}

	public void setOneresult(ArrayList<Double> oneresult)
	{
		this.oneresult = oneresult;
	}

	public void setLevel(int level)
	{
		this.level = level;
	}

	public void setIlongitude(long ilongitude)
	{
		this.ilongitude = ilongitude;
	}

	public void setIlatitude(long ilatitude)
	{
		this.ilatitude = ilatitude;
	}

	public void setGongcanIlongitude(long gongcanIlongitude)
	{
		this.gongcanIlongitude = gongcanIlongitude;
	}

	public void setGongcanIlatitud(long gongcanIlatitud)
	{
		this.gongcanIlatitud = gongcanIlatitud;
	}

}
