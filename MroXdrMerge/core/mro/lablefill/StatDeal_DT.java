package mro.lablefill;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.GridItem;
import StructData.StatFreqGrid;
import StructData.Stat_CellGrid_4G;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.MultiOutputMng;
import xdr.lablefill.CellGridData_4G;
import xdr.lablefill.DayDataDeal_23G;
import xdr.lablefill.DayDataDeal_4G;
import xdr.lablefill.GridData_Freq;
import xdr.lablefill.HourDataDeal_4G;
import xdr.lablefill.GridData_4G;
import xdr.lablefill.ResultHelper;

public class StatDeal_DT extends xdr.lablefill.StatDeal_DT
{

	public StatDeal_DT(MultiOutputMng<NullWritable, Text> mosMng)
	{
		super(mosMng);

		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_DT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_DT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_DT);
	}

	public StatDeal_DT(MultiOutputMng<NullWritable, Text> mosMng, int type)
	{
		super(mosMng);
		data_type = type;
		dayDataDeal_4G = new DayDataDeal_4G(STATDEAL_DT);
		dayDataDeal_23G = new DayDataDeal_23G(STATDEAL_DT);
		hourDataDeal_4G = new HourDataDeal_4G(STATDEAL_DT);
	}

	@Override
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdtgridDt", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("griddt", NullWritable.get(), curText);
						}
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("tenmdtgridDt", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("tengriddt", NullWritable.get(), curText);
						}
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
		if (dayDataDeal_4G.getGridFreqCount() > 10000)
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
							if (data_type == StaticConfig.DATA_TYPE_MDT)
							{
								mosMng.write("mdtgriddtfreq", NullWritable.get(), curText);
							}
							else
							{
								mosMng.write("griddtfreq", NullWritable.get(), curText);
							}
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
							if (data_type == StaticConfig.DATA_TYPE_MDT)
							{
								mosMng.write("mdttengriddtfreq", NullWritable.get(), curText);
							}
							else
							{
								mosMng.write("tengriddtfreq", NullWritable.get(), curText);
							}
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

		for (DayDataDeal_4G.DayDataItem gridTimeDeal : dayDataDeal_4G.getDayDataDealMap().values())
		{
			if (gridTimeDeal.getTen_LTfreqGridMap().size() > 10000)
			{
				for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_LTfreqGridMap().values())
				{
					try
					{
						curText.set(gridFreqMap.toLine());
						mosMng.write("LTtenFreqByImeiDt", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_LTfreqGridMap().clear();
			}
			if (gridTimeDeal.getTen_cellGridDataMap().size() > 10000)
			{
				for (StatFreqGrid gridFreqMap : gridTimeDeal.getTen_DXfreqGridMap().values())
				{
					try
					{
						curText.set(gridFreqMap.toLine());
						mosMng.write("DXtenFreqByImeiDt", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				gridTimeDeal.getTen_DXfreqGridMap().clear();

				for (Map.Entry<Long, CellGridData_4G> valuePare : gridTimeDeal.getTen_cellGridDataMap().entrySet())
				{
					valuePare.getValue().finalDeal();

					for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
					{
						try
						{
							curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
							if (data_type == StaticConfig.DATA_TYPE_MDT)
							{
								mosMng.write("tenmdtcellgridDt", NullWritable.get(), curText);
							}
							else
							{
								mosMng.write("tencellgriddt", NullWritable.get(), curText);
							}
						}
						catch (Exception e)
						{
							// TODO: handle exception
						}
					}
				}
				gridTimeDeal.getTen_cellGridDataMap().clear();
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
			for (Map.Entry<GridItem, GridData_4G> valuePare : gridTimeDeal.getGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();
				try
				{
					curText.set(ResultHelper.getPutGrid_4G(valuePare.getValue().getLteGrid()));
					if (data_type == StaticConfig.DATA_TYPE_MDT)
					{
						mosMng.write("mdtgridDt", NullWritable.get(), curText);
					}
					else
					{
						mosMng.write("griddt", NullWritable.get(), curText);
					}
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
					if (data_type == StaticConfig.DATA_TYPE_MDT)
					{
						mosMng.write("tenmdtgridDt", NullWritable.get(), curText);
					}
					else
					{
						mosMng.write("tengriddt", NullWritable.get(), curText);
					}
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_gridDataMap().clear();
		}

		// 输出栅格,基于一个imsi号的所有栅格结果都可以输出了
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdtgriddtfreq", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("griddtfreq", NullWritable.get(), curText);
						}
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
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("mdttengriddtfreq", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("tengriddtfreq", NullWritable.get(), curText);
						}
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
					mosMng.write("LTtenFreqByImeiDt", NullWritable.get(), curText);
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
					mosMng.write("DXtenFreqByImeiDt", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			gridTimeDeal.getTen_DXfreqGridMap().clear();

			for (Map.Entry<Long, CellGridData_4G> valuePare : gridTimeDeal.getTen_cellGridDataMap().entrySet())
			{
				valuePare.getValue().finalDeal();

				for (Stat_CellGrid_4G lteCellGrid : valuePare.getValue().getGridDataMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutCellGrid_4G(lteCellGrid));
						if (data_type == StaticConfig.DATA_TYPE_MDT)
						{
							mosMng.write("tenmdtcellgridDt", NullWritable.get(), curText);
						}
						else
						{
							mosMng.write("tencellgriddt", NullWritable.get(), curText);
						}
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

	}

}
