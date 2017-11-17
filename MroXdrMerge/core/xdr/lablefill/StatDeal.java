package xdr.lablefill;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Sample_23G;
import StructData.DT_Sample_4G;
import StructData.GridItem;
import StructData.Stat_CellGrid_23G;
import StructData.Stat_CellGrid_4G;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.data.MyInt;
import xdr.lablefill.HourDataDeal_4G.HourDataItem;;

public class StatDeal
{
	public static final int STATDEAL_DT = 1;
	public static final int STATDEAL_CQT = 2;
	public static final int STATDEAL_ALL = 3;

	protected MultiOutputMng<NullWritable, Text> mosMng;
	protected Text curText = new Text();

	protected DayDataDeal_4G dayDataDeal_4G;// time，天统计
	protected DayDataDeal_23G dayDataDeal_23G;// time，天统计
	protected HourDataDeal_4G hourDataDeal_4G;
	protected int curHourTime;
	protected int curDayTime;
	// 处理数据类型
	protected int data_type;

	public Map<Long, MyInt> cellLocDic = new HashMap<Long, MyInt>();

	public StatDeal(MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.mosMng = mosMng;

		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_ALL);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_ALL);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_ALL);
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
		// 天统计
		dayDataDeal_4G.dealSample(sample);
	}

	public void dealSample(DT_Sample_23G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		// 天统计
		dayDataDeal_23G.dealSample(sample);
	}

	public void outDealingData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 输出栅格统计结果
		if (dayDataDeal_4G.getGridCount() > 10000)
		{
			for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (GridData_4G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
						mosMng.write("xdrgrid", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}

		// 输出小区栅格
		if (dayDataDeal_4G.getCellGridCount() > 10000)
		{
			for (DayDataDeal_4G.DayDataItem dayDataDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (CellGridData_4G cellGridData : dayDataDeal.getCellGridDataMap().values())
				{
					cellGridData.finalDeal();
					for (Stat_CellGrid_4G lteCellGrid : cellGridData.getGridDataMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
							mosMng.write("xdrcellgrid", NullWritable.get(), curText);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}

				}
				dayDataDeal.getCellGridDataMap().clear();
			}
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 输出栅格统计结果
		if (dayDataDeal_23G.getGridCount() > 10000)
		{
			for (DayDataDeal_23G.DayDataItem gridTimeDeal : dayDataDeal_23G.getDayDataDealMap().values())
			{
				for (GridData_23G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_23G(gridData.getGridItem()));
						mosMng.write("xdrgrid23g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}

		// 输出小区栅格
		if (dayDataDeal_23G.getCellGridCount() > 10000)
		{
			for (DayDataDeal_23G.DayDataItem dayDataDeal : dayDataDeal_23G.getDayDataDealMap().values())
			{
				for (CellGridData_23G cellGridData : dayDataDeal.getCellGridDataMap().values())
				{
					cellGridData.finalDeal();
					for (Stat_CellGrid_23G lteCellGrid : cellGridData.getGridCellGridMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_23G(lteCellGrid));
							mosMng.write("xdrcellgrid23g", NullWritable.get(), curText);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}

				}
				dayDataDeal.getCellGridDataMap().clear();
			}
		}

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}

	public void outAllData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 小时数据吐出/////////////////////////////////////////////////////////////////////////////////////
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid()));
					mosMng.write("xdrgrid", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

		}

		// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem dayDataDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			// 输出小区天数据
			for (Map.Entry<Long, CellData_4G> valuePare : dayDataDeal.getCellDataMap().entrySet())
			{
				try
				{
					if (cellLocDic.containsKey(valuePare.getKey()))
					{
						valuePare.getValue().getLteCell().origLocXdrCount = cellLocDic.get(valuePare.getKey()).data;
					}

					curText.set(ResultHelper.getPutCell_4G(valuePare.getValue().getLteCell()));
					mosMng.write("xdrcell", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
						mosMng.write("xdrcellgrid", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
		}

		for (HourDataItem hourData : hourDataDeal_4G.getHourDataDealMap().values())
		{
			for (Map.Entry<GridItem, UserGridStat_4G> valuePare : hourData.getUserGirdDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutUserGridInfo(valuePare.getValue().getUserGrid()));
					mosMng.write("xdrgriduserhour", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		for (DayDataDeal_23G.DayDataItem gridTimeDeal : dayDataDeal_23G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_23G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_23G(valuePare.getValue().getGridItem()));
					mosMng.write("xdrgrid23g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

		}

		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_23G.DayDataItem dayDataDeal : dayDataDeal_23G.getDayDataDealMap().values())
		{

			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_23G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_23G lteCellGrid : valuePare.getValue().getGridCellGridMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_23G(lteCellGrid));
						mosMng.write("xdrcellgrid23g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
		}

		/////////////////////////////////////////////// 23G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}

}
