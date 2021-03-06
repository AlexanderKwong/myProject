package xdr.lablefill.by23g;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_3G;
import StructData.GridItem;
import StructData.Stat_Grid_3G;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import jan.util.TimeHelper;

public class DayDataDeal_3G
{

	private Map<Integer, DayDataItem> dayDataDealMap;// time，天统计
	private int curDayTime;
	
	public DayDataDeal_3G()
	{
		dayDataDealMap = new HashMap<Integer, DayDataItem>();
	}
	
	public void dealSample(DT_Sample_3G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		curDayTime = TimeHelper.getRoundDayTime(sample.itime);
		DayDataItem dayDataDeal = dayDataDealMap.get(curDayTime);
		if (dayDataDeal == null)
		{
			dayDataDeal = new DayDataItem(curDayTime);
			dayDataDealMap.put(curDayTime, dayDataDeal);
		}
		dayDataDeal.dealSample(sample);
	}
	
	
	public Map<Integer, DayDataItem> getDayDataDealMap()
	{
		return dayDataDealMap;
	}

	public int getGridCount()
	{
		int gridCount = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			gridCount += item.getGridDataMap().size();
		}
		return gridCount;
	}

	public int getCellCount()
	{
		int count = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			count += item.getCellDataMap().size();
		}
		return count;
	}

	public int getCellGridCount()
	{
		int gridCount = 0;
		for (DayDataItem item : dayDataDealMap.values())
		{
			for (CellGridData_3G cellGridData : item.getCellGridDataMap().values())
			{
				gridCount += cellGridData.getGridDataMap().size();
			}
		}
		return gridCount;
	}
	
	
	public class DayDataItem
	{
		private int statTime;
		private Map<Long, CellData_3G> cellDataMap;
		private Map<Long, CellGridData_3G> cellGridDataMap;
		private Map<GridItem, GridData_3G> gridDataMap;

		public DayDataItem(int statTime)
		{
			this.statTime = statTime;
			cellDataMap = new HashMap<Long, CellData_3G>();
			cellGridDataMap = new HashMap<Long, CellGridData_3G>();
			gridDataMap = new HashMap<GridItem, GridData_3G>();
		}

		public Map<Long, CellData_3G> getCellDataMap()
		{
			return cellDataMap;
		}

		public Map<Long, CellGridData_3G> getCellGridDataMap()
		{
			return cellGridDataMap;
		}

		public Map<GridItem, GridData_3G> getGridDataMap()
		{
			return gridDataMap;
		}

		public int getStatTime()
		{
			return statTime;
		}

		public void dealSample(DT_Sample_3G sample)
		{
			//小区统计是全量数据进行运算
			{// 小区统计

				if(sample.iLAC > 0 && sample.iCI > 0)
				{
					// 只统计mro的数据，mre不考虑
					if (sample.flag.toUpperCase().equals("MRO") || sample.flag.toUpperCase().equals("EVT"))
					{
						long cellKey = CellConfig.makeGsmCellKey(sample.iLAC, sample.iCI);
						CellData_3G cellData = cellDataMap.get(cellKey);
						if (cellData == null)
						{
							cellData = new CellData_3G(sample.cityID, sample.iLAC, sample.iCI, statTime,
									statTime + 86400);
							cellDataMap.put(cellKey, cellData);
						}
						cellData.dealSample(sample);
					}
				}

			}
			
			
			//小区栅格，栅格只算筛选的数据
			if (sample.testType == StaticConfig.TestType_DT 
					|| sample.testType == StaticConfig.TestType_CQT
					|| sample.testType == StaticConfig.TestType_DT_EX)
			{
				if (sample.iLAC > 0 && sample.iCI > 0)
				{
					{// 小区栅格统计
						long cellKey = CellConfig.makeGsmCellKey(sample.iLAC, sample.iCI);
						CellGridData_3G cellGridData = cellGridDataMap.get(cellKey);
						if (cellGridData == null)
						{
							cellGridData = new CellGridData_3G(sample.cityID, sample.iLAC, sample.iCI, statTime,
									statTime + 86400);
							cellGridDataMap.put(cellKey, cellGridData);
						}
						cellGridData.dealSample(sample);
					}

				}

				if (sample.ilongitude > 0 && sample.ilatitude > 0)
				{
					GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
					GridData_3G gridData = gridDataMap.get(gridItem);
					if (gridData == null)
					{
						gridData = new GridData_3G(statTime, statTime + 86400);

						Stat_Grid_3G lteGrid = gridData.getStatItem();
						lteGrid.icityid = sample.cityID;
						lteGrid.itllongitude = gridItem.getTLLongitude();
						lteGrid.itllatitude = gridItem.getTLLatitude();
						lteGrid.ibrlongitude = gridItem.getBRLongitude();
						lteGrid.ibrlatitude = gridItem.getBRLatitude();
						lteGrid.startTime = statTime;
						lteGrid.endTime = statTime + 86400;

						gridDataMap.put(gridItem, gridData);
					}
					gridData.dealSample(sample);
				}
			}

			
		}
	}
	
	
}
