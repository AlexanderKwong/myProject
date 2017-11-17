package xdr.lablefill.by23g;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_3G;
import StructData.GridItem;
import StructData.Stat_CellGrid_3G;
import jan.util.data.MyInt;

public class CellGridData_3G
{
	private int cityID;
	private int lac;
	private int ci;
	private int startTime;
	private int endTime;
	
	private Map<GridItem, Stat_CellGrid_3G> gridDataMap;
	
   
	public CellGridData_3G(int cityID, int lac, int ci, int startTime, int endTime)
	{
		this.cityID = cityID;
		this.lac = lac;
		this.ci = ci;
		this.startTime = startTime;
		this.endTime = endTime;
		
		gridDataMap = new HashMap<GridItem, Stat_CellGrid_3G>();
	}
	
	public Map<GridItem, Stat_CellGrid_3G> getGridDataMap()
	{
		return gridDataMap;
	}
	
	public void dealSample(DT_Sample_3G sample)
	{
		//小区栅格统计
		if (sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
			Stat_CellGrid_3G lteGrid = gridDataMap.get(gridItem);
			if (lteGrid == null)
			{
				lteGrid = new Stat_CellGrid_3G();
				
				lteGrid.icityid = sample.cityID;
				lteGrid.iLac = sample.iLAC;
				lteGrid.iCi = (int)sample.iCI;
				lteGrid.itllongitude = gridItem.getTLLongitude();
				lteGrid.itllatitude = gridItem.getTLLatitude();
				lteGrid.ibrlongitude = gridItem.getBRLongitude();
				lteGrid.ibrlatitude = gridItem.getBRLatitude();
				lteGrid.startTime = startTime;
				lteGrid.endTime = endTime;
				
				gridDataMap.put(gridItem, lteGrid);
			}


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
	}
	
	public void finalDeal()
	{
		for (Stat_CellGrid_3G item : gridDataMap.values())
		{
			item.UserCount = item.imsiMap.size();
		}
	}
   
   
}
