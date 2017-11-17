package mro.lablefillex_uemro;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.GridItem;
import StructData.StatFreqGrid;
import StructData.Stat_CellGrid_4G;
import jan.com.hadoop.mapred.MultiOutputMng;
import xdr.lablefill.CellGridData_4G;
import xdr.lablefill.DayDataDeal_23G;
import xdr.lablefill.DayDataDeal_4G;
import xdr.lablefill.GridData_Freq;
import xdr.lablefill.HourDataDeal_4G;
import xdr.lablefill.GridData_4G;
import xdr.lablefill.ResultHelper;

public class StatDeal_CQT extends xdr.lablefill.StatDeal_CQT
{

	public StatDeal_CQT(MultiOutputMng<NullWritable, Text> mosMng)
	{
		super(mosMng);

		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_CQT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_CQT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_CQT);
	}

	@Override
	public void outDealingData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 输出栅格统计结果
		if (dayDataDeal_4G.getGridCount() > 100000)
		{
			for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (GridData_4G gridData : gridTimeDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
						mosMng.write("gridcqt", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getGridDataMap().clear();

				for (GridData_4G gridData : gridTimeDeal.getTen_gridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
						mosMng.write("tengridcqt", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_gridDataMap().clear();
			}
		}

		// 输出栅格统计结果
		if (dayDataDeal_4G.getGridFreqCount() > 100000)
		{
			for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getGridDataFreqMap().values())
				{
					for (GridData_Freq gridData : gridFreqMap.values())
					{
						gridData.finalDeal();
						try
						{
							curText.set(ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid()));
							mosMng.write("gridcqtfreq", NullWritable.get(), curText);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				gridTimeDeal.getGridDataFreqMap().clear();
				for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getTen_gridDataFreqMap().values())
				{
					for (GridData_Freq gridData : gridFreqMap.values())
					{
						gridData.finalDeal();
						try
						{
							curText.set(ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid()));
							mosMng.write("tengridcqtfreq", NullWritable.get(), curText);
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}

				}
				gridTimeDeal.getTen_gridDataFreqMap().clear();
			}
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////
		dayDataDeal_4G.outData(false, mosMng, curText);
	}

	@Override
	public void outAllData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid()));
					mosMng.write("gridcqt", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getGridDataMap().clear();

			for (GridData_4G gridData : gridTimeDeal.getTen_gridDataMap().values())
			{
				gridData.finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
					mosMng.write("tengridcqt", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_gridDataMap().clear();
		}

		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getGridDataFreqMap().values())
			{
				for (GridData_Freq gridData : gridFreqMap.values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid()));
						mosMng.write("gridcqtfreq", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}

			}
			gridTimeDeal.getGridDataFreqMap().clear();
			for (Map<GridItem, GridData_Freq> gridFreqMap : gridTimeDeal.getTen_gridDataFreqMap().values())
			{
				for (GridData_Freq gridData : gridFreqMap.values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G_FREQ(gridData.getLteGrid()));
						mosMng.write("tengridcqtfreq", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
			gridTimeDeal.getTen_gridDataFreqMap().clear();

			// new freqgrid byImei

			for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_LTfreqGridMap().values())
			{
				try
				{
					curText.set(gridFreqMap.toLine());
					mosMng.write("LTtenFreqByImeiCqt", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_LTfreqGridMap().clear();

			for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_DXfreqGridMap().values())
			{
				try
				{
					curText.set(gridFreqMap.toLine());
					mosMng.write("DXtenFreqByImeiCqt", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_DXfreqGridMap().clear();

			// 输出cqt小区栅格数据

			for (Map.Entry<Long, CellGridData_4G> valuePare : gridTimeDeal.getTen_cellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
						mosMng.write("tencellgridcqt", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
			gridTimeDeal.getTen_cellGridDataMap().clear();
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////
		dayDataDeal_4G.outData(true, mosMng, curText);
	}

}
