package mro.lablefillex_uemro;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.CellFreqItem;
import StructData.GridItem;
import StructData.StatFreqCell;
import StructData.Stat_CellGrid_4G;
import jan.com.hadoop.mapred.MultiOutputMng;
import xdr.lablefill.CellData_4G;
import xdr.lablefill.CellData_Freq;
import xdr.lablefill.CellGridData_4G;
import xdr.lablefill.DayDataDeal_23G;
import xdr.lablefill.DayDataDeal_4G;
import xdr.lablefill.GridData_4G;
import xdr.lablefill.HourDataDeal_4G;
import xdr.lablefill.ResultHelper;

public class StatDeal extends xdr.lablefill.StatDeal
{

	public StatDeal(MultiOutputMng<NullWritable, Text> mosMng)
	{
		super(mosMng);

		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_ALL);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_ALL);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_ALL);
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
						mosMng.write("mrogrid", NullWritable.get(), curText);
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
						mosMng.write("tenmrogrid", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_gridDataMap().clear();
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
					mosMng.write("mrogrid", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

			for (GridData_4G gridData : gridTimeDeal.getTen_gridDataMap().values())
			{
				gridData.finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
					mosMng.write("tenmrogrid", NullWritable.get(), curText);
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
					mosMng.write("mrocell", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

			// 输出异频小区天数据
			for (Map.Entry<CellFreqItem, CellData_Freq> valuePare : dayDataDeal.getCellDataFreqMap().entrySet())
			{
				try
				{
					curText.set(ResultHelper.getPutCell_Freq(valuePare.getValue().getLteCell()));
					mosMng.write("mrocellfreq", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

			// 输出new freqCell byImei
			for (StatFreqCell valuePare : dayDataDeal.getFreqLTCellMap().values())
			{
				try
				{
					curText.set(valuePare.toLine());
					mosMng.write("LTfreqcellByImei", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

			for (StatFreqCell valuePare : dayDataDeal.getFreqDXCellMap().values())
			{
				try
				{
					curText.set(valuePare.toLine());
					mosMng.write("DXfreqcellByImei", NullWritable.get(), curText);
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
						mosMng.write("mrocellgrid", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}

			for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getTen_cellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
						mosMng.write("tenmrocellgrid", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
			}
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////
		dayDataDeal_4G.outData(true, mosMng, curText);
	}

}
