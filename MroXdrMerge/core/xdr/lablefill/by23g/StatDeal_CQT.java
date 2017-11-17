package xdr.lablefill.by23g;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Sample_2G;
import StructData.DT_Sample_3G;
import StructData.GridItem;
import StructData.Stat_CellGrid_2G;
import StructData.Stat_CellGrid_3G;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.MultiOutputMng;
import xdr.lablefill.ResultHelper;

public class StatDeal_CQT extends StatDeal
{
	public StatDeal_CQT(MultiOutputMng<NullWritable, Text> mosMng)
    {
    	super(mosMng);
    	
    }
	
	@Override
	public void dealSample(DT_Sample_2G sample)
	{
		if(sample.testType != StaticConfig.TestType_CQT)
		{
            return;
		}
		
		if (sample.itime == 0)
		{
			return;
		}
		
		// 天统计
		dayDataDeal_2G.dealSample(sample);
	}
	
	@Override
	public void dealSample(DT_Sample_3G sample)
	{
		if(sample.testType != StaticConfig.TestType_CQT)
		{
            return;
		}
		
		if (sample.itime == 0)
		{
			return;
		}
		
		// 天统计
		dayDataDeal_3G.dealSample(sample);
	}
	
	@Override
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
						mosMng.write("gridcqt2g", NullWritable.get(), curText);
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
							mosMng.write("cellgridcqt2g", NullWritable.get(), curText);
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
						mosMng.write("gridcqt3g", NullWritable.get(), curText);
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
							mosMng.write("cellgridcqt3g", NullWritable.get(), curText);
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
					mosMng.write("gridcqt2g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
		}

		for (DayDataDeal_2G.DayDataItem dayDataDeal : dayDataDeal_2G.getDayDataDealMap().values())
		{
			
			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_2G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				
				for (Stat_CellGrid_2G cellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_2G(cellGrid));
						mosMng.write("cellgridcqt2g", NullWritable.get(), curText);
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
					mosMng.write("gridcqt3g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
		}

		for (DayDataDeal_3G.DayDataItem dayDataDeal : dayDataDeal_3G.getDayDataDealMap().values())
		{
			
			// 输出小区栅格数据
			for (Map.Entry<Long, CellGridData_3G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				
				for (Stat_CellGrid_3G cellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_3G(cellGrid));
						mosMng.write("cellgridcqt3g", NullWritable.get(), curText);
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
