package xdr.lablefill.by23g;

import StructData.DT_Sample_2G;
import StructData.Stat_Cell_2G;

public class CellData_2G
{
	private int lac;
	private long ci;
	private int startTime;
	private int endTime;
	private Stat_Cell_2G statItem;

	public CellData_2G(int cityID, int lac, long ci, int startTime, int endTime)
	{
		this.lac = lac;
		this.ci = ci;
		this.startTime = startTime;
		this.endTime = endTime;
		
		statItem = new Stat_Cell_2G();
		statItem.Clear();
		
		statItem.icityid = cityID;
		statItem.startTime = startTime;
		statItem.endTime = endTime;
		statItem.iLAC = lac;
		statItem.wRAC = 0;
		statItem.iCI = ci;
	}

	public int getLac()
	{
		return lac;
	}

	public long getEci()
	{
		return ci;
	}
	
	public Stat_Cell_2G getStatItem()
	{
		return statItem;
	}

	public void dealSample(DT_Sample_2G sample)
	{
		boolean isSampleMro = sample.flag.toUpperCase().equals("MRO");
		boolean isSampleMre = sample.flag.toUpperCase().equals("MRE");		
		
		//小区统计
		statItem.iduration += sample.duration;		
		if (isSampleMro || isSampleMre)
		{
			statItem.isamplenum++;

			if(isSampleMro)
			{

			}
			else if(isSampleMre)
			{

			}		
		}
		else
		{
			statItem.xdrCount++;
			
		}
	
	}


}

