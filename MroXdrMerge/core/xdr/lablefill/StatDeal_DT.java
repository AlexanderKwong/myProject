package xdr.lablefill;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Sample_23G;
import StructData.DT_Sample_4G;
import StructData.GridItem;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.MultiOutputMng;

public class StatDeal_DT extends StatDeal
{
	
	public StatDeal_DT(MultiOutputMng<NullWritable, Text> mosMng)
	{
		super(mosMng);
		
		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_DT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_DT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_DT);
	}
	
	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		if(sample.testType != StaticConfig.TestType_DT)
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

		if(sample.testType != StaticConfig.TestType_DT)
		{
            return;
		}

		// 天统计
		dayDataDeal_23G.dealSample(sample);
	}
	
	public void outDealingData()
	{
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
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
						mosMng.write("griddt", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}
		
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////
		
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
						mosMng.write("griddt23g", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();
			}
		}
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////

	}

	public void outAllData()
	{
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid()));
					mosMng.write("griddt", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
		}
		
        /////////////////////////////////////////////// 4G /////////////////////////////////////////////////
		
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////
		
		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_23G.DayDataItem gridTimeDeal : dayDataDeal_23G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_23G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_23G(valuePare.getValue().getGridItem()));
					mosMng.write("griddt23g", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			
		}
		
        /////////////////////////////////////////////// 23G /////////////////////////////////////////////////		
			
	}
	
	
}
