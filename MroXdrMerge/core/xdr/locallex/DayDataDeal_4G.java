package xdr.locallex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.GridItemOfSize;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.IWriteLogCallBack.LogType;
import mroxdrmerge.MainModel;
import jan.util.LOGHelper;
import jan.util.TimeHelper;

public class DayDataDeal_4G
{
	private Map<Integer, DayDataItem> dayDataDealMap;// time，天统计
	private int curDayTime;
	private MultiOutputMng<NullWritable, Text> mosMng;

	public DayDataDeal_4G(MultiOutputMng<NullWritable, Text> mosMng)
	{
		dayDataDealMap = new HashMap<Integer, DayDataItem>();
		this.mosMng = mosMng;
	}

	public void dealEvent(EventData event)
	{
		if (event.iTime <= 0)
		{
			return;
		}

		curDayTime = TimeHelper.getRoundDayTime(event.iTime);
		DayDataItem dayDataDeal = dayDataDealMap.get(curDayTime);
		if (dayDataDeal == null)
		{
			dayDataDeal = new DayDataItem(mosMng, curDayTime, curDayTime + 86400);
			dayDataDealMap.put(curDayTime, dayDataDeal);
		}
		dayDataDeal.dealEvent(event);
	}

	public Map<Integer, DayDataItem> getDayDataDealMap()
	{
		return dayDataDealMap;
	}

	public int outResult()
	{
		for (DayDataItem item : dayDataDealMap.values())
		{

			item.outResult();
		}
		return 0;
	}

	public class DayDataItem
	{
		private int stime;
		private int etime;
		private MultiOutputMng<NullWritable, Text> mosMng;

		private List<AStatDo> statdoList = new ArrayList<AStatDo>();

		private AStatDo stat_TB_EVENT_CELL;

		private AStatDo stat_TB_EVENT_HIGH_IN_CELLGRID;
		private AStatDo stat_TB_EVENT_MID_IN_CELLGRID;
		private AStatDo stat_TB_EVENT_LOW_IN_CELLGRID;

		private AStatDo stat_TB_EVENT_HIGH_IN_GRID;
		private AStatDo stat_TB_EVENT_MID_IN_GRID;
		private AStatDo stat_TB_EVENT_LOW_IN_GRID;

		private AStatDo stat_TB_EVENT_HIGH_OUT_CELLGRID;
		private AStatDo stat_TB_EVENT_MID_OUT_CELLGRID;
		private AStatDo stat_TB_EVENT_LOW_OUT_CELLGRID;

		private AStatDo stat_TB_EVENT_HIGH_OUT_GRID;
		private AStatDo stat_TB_EVENT_MID_OUT_GRID;
		private AStatDo stat_TB_EVENT_LOW_OUT_GRID;

		// 按照楼宇维度进行统计
		private AStatDo stat_TB_EVENT_HIGH_Build_CELLGRID;
		private AStatDo stat_TB_EVENT_MID_Build_CELLGRID;
		private AStatDo stat_TB_EVENT_LOW_Build_CELLGRID;

		private AStatDo stat_TB_EVENT_HIGH_Build_GRID;
		private AStatDo stat_TB_EVENT_MID_Build_GRID;
		private AStatDo stat_TB_EVENT_LOW_Build_GRID;
		
		//高铁场景统计
		private AStatDo stat_TB_EVENT_Area;
		private AStatDo stat_TB_EVENT_Area_GRID;
		private AStatDo stat_TB_EVENT_Area_CELLGRID;
		private AStatDo stat_TB_EVENT_Area_CELL;
		

		public DayDataItem(MultiOutputMng<NullWritable, Text> mosMng, int stime, int etime)
		{
			this.stime = stime;
			this.etime = etime;
			this.mosMng = mosMng;

			stat_TB_EVENT_CELL = new EventDataCellStat(stime, etime, "tbEventCell", mosMng);
			statdoList.add(stat_TB_EVENT_CELL);

			stat_TB_EVENT_HIGH_IN_CELLGRID = new EventDataInCellGridStat(stime, etime, "tbEventHighInCellGrid", mosMng);
			statdoList.add(stat_TB_EVENT_HIGH_IN_CELLGRID);
			stat_TB_EVENT_MID_IN_CELLGRID = new EventDataInCellGridStat(stime, etime, "tbEventMidInCellGrid", mosMng);
			statdoList.add(stat_TB_EVENT_MID_IN_CELLGRID);
			stat_TB_EVENT_LOW_IN_CELLGRID = new EventDataInCellGridStat(stime, etime, "tbEventLowInCellGrid", mosMng);
			statdoList.add(stat_TB_EVENT_LOW_IN_CELLGRID);

			stat_TB_EVENT_HIGH_IN_GRID = new EventDataInGridStat(stime, etime, "tbEventHighInGrid", mosMng);
			statdoList.add(stat_TB_EVENT_HIGH_IN_GRID);
			stat_TB_EVENT_MID_IN_GRID = new EventDataInGridStat(stime, etime, "tbEventMidInGrid", mosMng);
			statdoList.add(stat_TB_EVENT_MID_IN_GRID);
			stat_TB_EVENT_LOW_IN_GRID = new EventDataInGridStat(stime, etime, "tbEventLowInGrid", mosMng);
			statdoList.add(stat_TB_EVENT_LOW_IN_GRID);

			stat_TB_EVENT_HIGH_OUT_CELLGRID = new EventDataOutCellGridStat(stime, etime, "tbEventHighOutCellGrid",
					mosMng);
			statdoList.add(stat_TB_EVENT_HIGH_OUT_CELLGRID);
			stat_TB_EVENT_MID_OUT_CELLGRID = new EventDataOutCellGridStat(stime, etime, "tbEventMidOutCellGrid",
					mosMng);
			statdoList.add(stat_TB_EVENT_MID_OUT_CELLGRID);
			stat_TB_EVENT_LOW_OUT_CELLGRID = new EventDataOutCellGridStat(stime, etime, "tbEventLowOutCellGrid",
					mosMng);
			statdoList.add(stat_TB_EVENT_LOW_OUT_CELLGRID);

			stat_TB_EVENT_HIGH_OUT_GRID = new EventDataOutGridStat(stime, etime, "tbEventHighOutGrid", mosMng);
			statdoList.add(stat_TB_EVENT_HIGH_OUT_GRID);
			stat_TB_EVENT_MID_OUT_GRID = new EventDataOutGridStat(stime, etime, "tbEventMidOutGrid", mosMng);
			statdoList.add(stat_TB_EVENT_MID_OUT_GRID);
			stat_TB_EVENT_LOW_OUT_GRID = new EventDataOutGridStat(stime, etime, "tbEventLowOutGrid", mosMng);
			statdoList.add(stat_TB_EVENT_LOW_OUT_GRID);

			// build cell_grid
			stat_TB_EVENT_HIGH_Build_CELLGRID = new EventDataBuildCellGridStat(stime, etime, "tbEventHighBuildCellGrid",
					mosMng);
			statdoList.add(stat_TB_EVENT_HIGH_Build_CELLGRID);
			stat_TB_EVENT_MID_Build_CELLGRID = new EventDataBuildCellGridStat(stime, etime, "tbEventMidBuildCellGrid",
					mosMng);
			statdoList.add(stat_TB_EVENT_MID_Build_CELLGRID);
			stat_TB_EVENT_LOW_Build_CELLGRID = new EventDataBuildCellGridStat(stime, etime, "tbEventLowBuildCellGrid",
					mosMng);
			statdoList.add(stat_TB_EVENT_LOW_Build_CELLGRID);

			// build grid
			stat_TB_EVENT_HIGH_Build_GRID = new EventDataBuildGridStat(stime, etime, "tbEventHighBuildGrid", mosMng);
			statdoList.add(stat_TB_EVENT_HIGH_Build_GRID);
			stat_TB_EVENT_MID_Build_GRID = new EventDataBuildGridStat(stime, etime, "tbEventMidBuildGrid", mosMng);
			statdoList.add(stat_TB_EVENT_MID_Build_GRID);
			stat_TB_EVENT_LOW_Build_GRID = new EventDataBuildGridStat(stime, etime, "tbEventLowBuildGrid", mosMng);
			statdoList.add(stat_TB_EVENT_LOW_Build_GRID);
			
			//高铁场景
			stat_TB_EVENT_Area = new EventDataAreaStat(stime, etime, "tbArea", mosMng);
			statdoList.add(stat_TB_EVENT_Area);
			stat_TB_EVENT_Area_GRID = new EventDataAreaGridStat(stime, etime, "tbAreaGrid", mosMng);
			statdoList.add(stat_TB_EVENT_Area_GRID);
			stat_TB_EVENT_Area_CELLGRID = new EventDataAreaCellGridStat(stime, etime, "tbAreaGridCell", mosMng);
			statdoList.add(stat_TB_EVENT_Area_CELLGRID);
			stat_TB_EVENT_Area_CELL = new EventDataAreaCellStat(stime, etime, "tbAreaCell", mosMng);
			statdoList.add(stat_TB_EVENT_Area_CELL);
		}

		public void outResult()
		{
			for (AStatDo item : statdoList)
			{
				if (item.outFinalReuslt() != 0)
				{
					// 这次不成功的话，应该有日志了吧
					LOGHelper.GetLogger().writeLog(LogType.error,
							"outFinalReuslt error: " + item.getClass().toString());
				}
			}
		}

		public void dealEvent(EventData event)
		{
			int indoorSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());
			int outdoorSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getOutDoorSize());

			if (event.confidentType == StaticConfig.OH)
			{

				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize);

				stat_TB_EVENT_HIGH_OUT_CELLGRID.stat(event);
				stat_TB_EVENT_HIGH_OUT_GRID.stat(event);

			}
			else if (event.confidentType == StaticConfig.OM)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize);

				stat_TB_EVENT_MID_OUT_CELLGRID.stat(event);
				stat_TB_EVENT_MID_OUT_GRID.stat(event);
			}
			else if (event.confidentType == StaticConfig.OL)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize);

				stat_TB_EVENT_LOW_OUT_CELLGRID.stat(event);
				stat_TB_EVENT_LOW_OUT_GRID.stat(event);
			}
			else if (event.confidentType == StaticConfig.IH)
			{

				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, indoorSize);

				stat_TB_EVENT_HIGH_IN_CELLGRID.stat(event);
				stat_TB_EVENT_HIGH_IN_GRID.stat(event);

				stat_TB_EVENT_HIGH_Build_GRID.stat(event);
				stat_TB_EVENT_HIGH_Build_CELLGRID.stat(event);

			}
			else if (event.confidentType == StaticConfig.IM)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, indoorSize);

				stat_TB_EVENT_MID_IN_CELLGRID.stat(event);
				stat_TB_EVENT_MID_IN_GRID.stat(event);

				stat_TB_EVENT_MID_Build_GRID.stat(event);
				stat_TB_EVENT_MID_Build_CELLGRID.stat(event);
			}
			else if (event.confidentType == StaticConfig.IL)
			{
				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, indoorSize);

				stat_TB_EVENT_LOW_IN_CELLGRID.stat(event);
				stat_TB_EVENT_LOW_IN_GRID.stat(event);

				stat_TB_EVENT_LOW_Build_GRID.stat(event);
				stat_TB_EVENT_LOW_Build_CELLGRID.stat(event);
			}

			stat_TB_EVENT_CELL.stat(event);
			
			//高铁场景统计
			if(event.iTestType == StaticConfig.TestType_HiRail){

				event.gridItem = new GridItemOfSize(-1, event.iLongitude, event.iLatitude, outdoorSize); 
				stat_TB_EVENT_Area.stat(event);
				stat_TB_EVENT_Area_GRID.stat(event);
				stat_TB_EVENT_Area_CELLGRID.stat(event);
				stat_TB_EVENT_Area_CELL.stat(event);
			}

		}
	}

}
