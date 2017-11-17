package mro.lablefill;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.CellFreqItem;
import StructData.GridItem;
import StructData.StatFreqCell;
import StructData.Stat_CellGrid_4G;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
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

	public StatDeal(MultiOutputMng<NullWritable, Text> mosMng, int type)
	{
		super(mosMng);
		data_type = type;
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
		if (dayDataDeal_4G.getGridCount() > 10000)
		{
			for (DayDataDeal_4G.DayDataItem dayDataDeal : dayDataDeal_4G.getDayDataDealMap().values())
			{
				for (GridData_4G gridData : dayDataDeal.getGridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdtgrid", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("mrogrid", NullWritable.get(), curText);
						}
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				dayDataDeal.getGridDataMap().clear();

				for (GridData_4G gridData : dayDataDeal.getTen_gridDataMap().values())
				{
					gridData.finalDeal();
					try
					{
						curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("tenmdtgrid", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("tenmrogrid", NullWritable.get(), curText);
						}
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				dayDataDeal.getTen_gridDataMap().clear();

				// ------
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdtcell", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("mrocell", NullWritable.get(), curText);
						}
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				dayDataDeal.getCellDataMap().clear();
				// 输出异频小区天数据
				for (Map.Entry<CellFreqItem, CellData_Freq> valuePare : dayDataDeal.getCellDataFreqMap().entrySet())
				{
					try
					{
						curText.set(ResultHelper.getPutCell_Freq(valuePare.getValue().getLteCell()));
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdtcellfreq", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("mrocellfreq", NullWritable.get(), curText);
						}
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				dayDataDeal.getCellDataFreqMap().clear();
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
				dayDataDeal.getFreqLTCellMap().clear();
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
				dayDataDeal.getFreqDXCellMap().clear();

				// 输出小区栅格数据
				for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getCellGridDataMap().entrySet())
				{
					valuePare.getValue().finalDeal();

					for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
							if (data_type == StaticConfig.DATA_TYPE_MDT)
							{
								mosMng.write("mdtcellgrid", NullWritable.get(), curText);
							}
							else
							{
								mosMng.write("mrocellgrid", NullWritable.get(), curText);
							}
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				dayDataDeal.getCellGridDataMap().clear();
				for (Map.Entry<Long, CellGridData_4G> valuePare : dayDataDeal.getTen_cellGridDataMap().entrySet())
				{
					valuePare.getValue().finalDeal();

					for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
							if (data_type == StaticConfig.DATA_TYPE_MDT)
							{
								mosMng.write("tenmdtcellgrid", NullWritable.get(), curText);
							}
							else
							{
								mosMng.write("tenmrocellgrid", NullWritable.get(), curText);
							}
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				dayDataDeal.getTen_cellGridDataMap().clear();
			}
		}

		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

	}

	@Override
	public void outAllData()
	{
		/////////////////////////////////////////////// 4G
		/////////////////////////////////////////////// /////////////////////////////////////////////////

		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "gridTimeDeal.getGridDataMap().size=" + gridTimeDeal.getGridDataMap().size());
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid()));
					if (data_type == StaticConfig.DATA_TYPE_MDT)
					{
						mosMng.write("mdtgrid", NullWritable.get(), curText);
					}
					else
					{
						mosMng.write("mrogrid", NullWritable.get(), curText);
					}
				}
				catch (Exception e)
				{
					// TODO: handle exception
					LOGHelper.GetLogger().writeLog(LogType.error, "错误信息 ", e);
				}
			}

			for (GridData_4G gridData : gridTimeDeal.getTen_gridDataMap().values())
			{
				gridData.finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(gridData.getLteGrid()));
					if (data_type == StaticConfig.DATA_TYPE_MDT)
					{
						mosMng.write("tenmdtgrid", NullWritable.get(), curText);
					}
					else
					{
						mosMng.write("tenmrogrid", NullWritable.get(), curText);
					}
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
					if (data_type == StaticConfig.DATA_TYPE_MDT)
					{
						mosMng.write("mdtcell", NullWritable.get(), curText);
					}
					else
					{
						mosMng.write("mrocell", NullWritable.get(), curText);
					}
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
					if (data_type == StaticConfig.DATA_TYPE_MDT)
					{
						mosMng.write("mdtcellfreq", NullWritable.get(), curText);
					}
					else
					{
						mosMng.write("mrocellfreq", NullWritable.get(), curText);
					}
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdtcellgrid", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("mrocellgrid", NullWritable.get(), curText);
						}
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("tenmdtcellgrid", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("tenmrocellgrid", NullWritable.get(), curText);
						}
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

	}

}
