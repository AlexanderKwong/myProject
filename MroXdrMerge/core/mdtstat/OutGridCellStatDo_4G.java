package mdtstat;

import java.util.HashMap;
import java.util.Map;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.struct.Stat_mdt_Out_CellGrid;
import mrstat.AStatDo;
import mrstat.TypeResult;

public class OutGridCellStatDo_4G extends AStatDo
{
	private Map<String, Stat_mdt_Out_CellGrid> high_out_cellgridDataMap;
	private Map<String, Stat_mdt_Out_CellGrid> low_out_cellgridDataMap;

	public OutGridCellStatDo_4G(TypeResult typeResult)
	{
		super(typeResult);
		// TODO Auto-generated constructor stub
		high_out_cellgridDataMap = new HashMap<String, Stat_mdt_Out_CellGrid>(); // cityid_longtitude_latitude作为key
		low_out_cellgridDataMap = new HashMap<String, Stat_mdt_Out_CellGrid>();
	}

	@Override
	public int statSub(Object tsam)
	{
		// TODO Auto-generated method stub
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		// if (sample.samState == StaticConfig.ACTTYPE_OUT)
		// {
		if (sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			String key = sample.cityID + "_" + Util.RoundTimeForHour(sample.itime) + "_" + sample.Eci + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_"
					+ sample.mrType;
			int locSource = sample.locSource;// 经纬度来源
			if (sample.ConfidenceType == StaticConfig.OH)
			{
				Stat_mdt_Out_CellGrid outCellGrid = high_out_cellgridDataMap.get(key);
				if (outCellGrid == null)
				{
					outCellGrid = new Stat_mdt_Out_CellGrid();
					outCellGrid.doFirstSample(sample);
					high_out_cellgridDataMap.put(key, outCellGrid);
				}
				outCellGrid.doSample(sample);
			}
			else if (sample.ConfidenceType == StaticConfig.OL)
			{
				Stat_mdt_Out_CellGrid outCellGrid = low_out_cellgridDataMap.get(key);
				if (outCellGrid == null)
				{
					outCellGrid = new Stat_mdt_Out_CellGrid();
					outCellGrid.doFirstSample(sample);
					low_out_cellgridDataMap.put(key, outCellGrid);
				}
				outCellGrid.doSample(sample);
			}
		}
		// }
		return 0;
	}

	// /**
	// * 专门统计iASNei_MRCnt fASNei_RSRPValue
	// *
	// * @param sample
	// */
	// public void doNCStatisitc(DT_Sample_4G sample, Map<String,
	// Stat_mdt_Out_CellGrid> out_cellGridDataMap)
	// {
	// for (int i = 0; i < sample.tlte.length; i++)
	// {
	// if (sample.tlte[i].LteNcEarfcn == StaticConfig.Int_Abnormal)
	// {
	// break;
	// }
	// LteCellInfo lteinfo =
	// CellConfig.GetInstance().getNearestCell(sample.ilongitude,
	// sample.ilatitude, sample.cityID, sample.tlte[i].LteNcEarfcn,
	// sample.tlte[i].LteNcPci);
	// String ncKey = "";
	// if (lteinfo != null)
	// {
	// ncKey = sample.cityID + "_" + lteinfo.eci + "_" + (sample.ilongitude /
	// 2000) * 2000 + "_" + (sample.ilatitude / 1800) * 1800;
	// Stat_mdt_Out_CellGrid outCellGrid = out_cellGridDataMap.get(ncKey);
	// if (outCellGrid == null)
	// {
	// outCellGrid = new Stat_mdt_Out_CellGrid();
	// outCellGrid.doFirstSample(sample);
	// out_cellGridDataMap.put(ncKey, outCellGrid);
	// }
	// // outCellGrid.doNcRsrp(sample.tlte[i].LteNcRSRP);
	// }
	// }
	// }

	@Override
	public int outDealingResultSub()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		// TODO Auto-generated method stub
		for (Stat_mdt_Out_CellGrid item : high_out_cellgridDataMap.values())
		{
			typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_OUTGRID_CELL_HIGH, item.toLine());
		}
		for (Stat_mdt_Out_CellGrid item : low_out_cellgridDataMap.values())
		{
			typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_OUTGRID_CELL_LOW, item.toLine());
		}
		high_out_cellgridDataMap.clear();
		low_out_cellgridDataMap.clear();
		return 0;
	}

}
