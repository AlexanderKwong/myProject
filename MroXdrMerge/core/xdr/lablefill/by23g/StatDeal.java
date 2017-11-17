package xdr.lablefill.by23g;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Sample_2G;
import StructData.DT_Sample_3G;
import StructData.GridItem;
import StructData.Stat_CellGrid_2G;
import StructData.Stat_CellGrid_3G;
import jan.com.hadoop.mapred.MultiOutputMng;
import xdr.lablefill.ResultHelper;;

public class StatDeal
{
	protected MultiOutputMng<NullWritable, Text> mosMng;
	protected Text curText = new Text();
	
	protected DayDataDeal_2G dayDataDeal_2G;// time，天统计
	protected DayDataDeal_3G dayDataDeal_3G;// time，天统计
	protected int curHourTime;
	protected int curDayTime;

	public StatDeal(MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.mosMng = mosMng;
		
		dayDataDeal_2G = new DayDataDeal_2G();
		dayDataDeal_3G = new DayDataDeal_3G();
		
	}

    public DayDataDeal_2G getDayDataDeal_2G()
	{
		return dayDataDeal_2G;
	}
    
    public DayDataDeal_3G getDayDataDeal_3G()
	{
		return dayDataDeal_3G;
	}
	
	public void dealSample(DT_Sample_2G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		// 天统计
		dayDataDeal_2G.dealSample(sample);
	}
	
	public void dealSample(DT_Sample_3G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		// 天统计
		dayDataDeal_3G.dealSample(sample);
	}
	
	public void outDealingData()
	{
		///////////////////////////////////////////// 2G ////////////////////////////////////////////////////
	
		// 输出栅格统计结果
		if (dayDataDeal_2G.getGridCount() > 10000)
		{
			for (DayDataDeal_2G.DayDataItem gridTimeDeal : dayDataDeal_2G.getDayDataDealMap().values())
			{
				for (GridData_2G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_2G(gridData.getStatItem()));
						mosMng.write("grid2g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}
		
		//输出小区栅格
		if (dayDataDeal_2G.getCellGridCount() > 100000)
		{
			for(DayDataDeal_2G.DayDataItem dayDataDeal : dayDataDeal_2G.getDayDataDealMap().values())
			{
				for (CellGridData_2G cellGridData : dayDataDeal.getCellGridDataMap().values())
				{
					cellGridData.finalDeal();						
					for(Stat_CellGrid_2G lteCellGrid : cellGridData.getGridDataMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_2G(lteCellGrid));
							mosMng.write("cellgrid2g", NullWritable.get(), curText);
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

        ///////////////////////////////////////////// 2G ////////////////////////////////////////////////////
		
		
		///////////////////////////////////////////// 3G ////////////////////////////////////////////////////
		
		// 输出栅格统计结果
		if (dayDataDeal_3G.getGridCount() > 10000)
		{
			for (DayDataDeal_3G.DayDataItem gridTimeDeal : dayDataDeal_3G.getDayDataDealMap().values())
			{
				for (GridData_3G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_3G(gridData.getStatItem()));
						mosMng.write("grid3g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}
		
		//输出小区栅格
		if (dayDataDeal_3G.getCellGridCount() > 100000)
		{
			for(DayDataDeal_3G.DayDataItem dayDataDeal : dayDataDeal_3G.getDayDataDealMap().values())
			{
				for (CellGridData_3G cellGridData : dayDataDeal.getCellGridDataMap().values())
				{
					cellGridData.finalDeal();						
					for(Stat_CellGrid_3G lteCellGrid : cellGridData.getGridDataMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_3G(lteCellGrid));
							mosMng.write("cellgrid3g", NullWritable.get(), curText);
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

        ///////////////////////////////////////////// 3G ////////////////////////////////////////////////////
	
	}

	public void outAllData()
	{
        ///////////////////////////////////////////// 2G ////////////////////////////////////////////////////
		for (DayDataDeal_2G.DayDataItem gridTimeDeal : dayDataDeal_2G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_2G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_2G(valuePare.getValue().getStatItem()));
					mosMng.write("grid2g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
		}

		for (DayDataDeal_2G.DayDataItem dayDataDeal : dayDataDeal_2G.getDayDataDealMap().values())
		{
			// 输出小区天数据
			for (Map.Entry<Long, CellData_2G> valuePare : dayDataDeal.getCellDataMap().entrySet())
			{
				try
				{								
					curText.set(ResultHelper.getPutCell_2G(valuePare.getValue().getStatItem()));
					mosMng.write("cell2g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_2G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				
				for (Stat_CellGrid_2G cellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_2G(cellGrid));
						mosMng.write("cellgrid2g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
		}
		
		///////////////////////////////////////////// 2G ////////////////////////////////////////////////////
		
		
        ///////////////////////////////////////////// 3G ////////////////////////////////////////////////////
		for (DayDataDeal_3G.DayDataItem gridTimeDeal : dayDataDeal_3G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_3G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_3G(valuePare.getValue().getStatItem()));
					mosMng.write("grid3g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
		}

		for (DayDataDeal_3G.DayDataItem dayDataDeal : dayDataDeal_3G.getDayDataDealMap().values())
		{
			// 输出小区天数据
			for (Map.Entry<Long, CellData_3G> valuePare : dayDataDeal.getCellDataMap().entrySet())
			{
				try
				{								
					curText.set(ResultHelper.getPutCell_3G(valuePare.getValue().getStatItem()));
					mosMng.write("cell3g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_3G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				
				for (Stat_CellGrid_3G cellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_3G(cellGrid));
						mosMng.write("cellgrid3g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
		}
		
		///////////////////////////////////////////// 3G ////////////////////////////////////////////////////
		
		
	}

}
