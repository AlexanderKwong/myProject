package xdr.lablefill.by23g;

import StructData.DT_Sample_3G;
import StructData.Stat_Grid_3G;
import jan.util.data.MyInt;

public class GridData_3G
{
	private int startTime;
	private int endTime;
	private Stat_Grid_3G lteGrid; 
	
	public GridData_3G(int startTime, int endTime)
	{
	   this.startTime = startTime;
	   this.endTime = endTime;
	   lteGrid  = new Stat_Grid_3G();
	}
	
	public Stat_Grid_3G getStatItem()
	{
		return lteGrid;
	}
	
	public int getStartTime()
	{
		return startTime;
	}
	
	public int getEndTime()
	{
		return endTime;
	}
	
	public void dealSample(DT_Sample_3G sample)
	{
		boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
		boolean isMreSample = sample.flag.toUpperCase().equals("MRE");
			
		lteGrid.isamplenum++;
		if (isMroSample || isMreSample)
		{
	
		}
		else
		{
			lteGrid.XdrCount++;
			lteGrid.iduration += sample.duration;
			
			//只有xdr，才算用户的个数，mr不用算
			if(sample.IMSI > 0)
			{
				MyInt item = lteGrid.imsiMap.get(sample.IMSI);
				if(item == null)
				{
					item = new MyInt(0);
					lteGrid.imsiMap.put(sample.IMSI, item);
				}
				item.data++;
			}
		}
	}
	
	public void finalDeal()
	{
		lteGrid.UserCount = lteGrid.imsiMap.size();
	}
	
}
