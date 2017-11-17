package mdtstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.GridItemOfSize;
import StructData.StaticConfig;
import mdtstat.struct.Stat_mdt_In_CellGrid;
import mrstat.AStatDo;
import mrstat.TypeResult;

public class InGridCellStatDo_4G extends AStatDo
{
	private Map<String, Stat_mdt_In_CellGrid> high_in_cellgridDataMap;
	private Map<String, Stat_mdt_In_CellGrid> low_in_cellgridDataMap;

	public InGridCellStatDo_4G(TypeResult typeResult)
	{
		super(typeResult);
		high_in_cellgridDataMap = new HashMap<String, Stat_mdt_In_CellGrid>(); // cityid_longtitude_latitude作为key
		low_in_cellgridDataMap = new HashMap<String, Stat_mdt_In_CellGrid>();
	}

	@Override
	public int statSub(Object tsam)
	{
		// TODO Auto-generated method stub
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		// if (sample.samState == StaticConfig.ACTTYPE_IN)
		// {
		if (sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			String key = sample.cityID + "_" + sample.Eci + "_" + sample.ispeed + "_" + sample.imode + "_" + Util.RoundTimeForHour(sample.itime) + "_" + sample.grid.tllongitude + "_"
					+ sample.grid.tllatitude + "_" + sample.mrType;
			int locSource = sample.locSource;// 经纬度来源
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_mdt_In_CellGrid inCellGrid = high_in_cellgridDataMap.get(key);
				if (inCellGrid == null)
				{
					inCellGrid = new Stat_mdt_In_CellGrid();
					inCellGrid.doFirstSample(sample);
					high_in_cellgridDataMap.put(key, inCellGrid);
				}
				inCellGrid.doSample(sample);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_mdt_In_CellGrid inCellGrid = low_in_cellgridDataMap.get(key);
				if (inCellGrid == null)
				{
					inCellGrid = new Stat_mdt_In_CellGrid();
					inCellGrid.doFirstSample(sample);
					low_in_cellgridDataMap.put(key, inCellGrid);
				}
				inCellGrid.doSample(sample);
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
	// Stat_mdt_In_CellGrid> in_cellGridDataMap)
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
	// 4000) * 4000 + "_" + (sample.ilatitude / 3600) * 3600;
	// Stat_mdt_In_CellGrid inCellGrid = in_cellGridDataMap.get(ncKey);
	// if (inCellGrid == null)
	// {
	// inCellGrid = new Stat_mdt_In_CellGrid();
	// inCellGrid.doFirstSample(sample);
	// in_cellGridDataMap.put(ncKey, inCellGrid);
	// }
	// // inCellGrid.doNcRsrp(sample.tlte[i].LteNcRSRP);
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
		for (Stat_mdt_In_CellGrid item : high_in_cellgridDataMap.values())
		{
			typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_INGRID_CELL_HIGH, item.toLine());
		}
		for (Stat_mdt_In_CellGrid item : low_in_cellgridDataMap.values())
		{
			typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_INGRID_CELL_LOW, item.toLine());
		}
		high_in_cellgridDataMap.clear();
		low_in_cellgridDataMap.clear();
		return 0;
	}

}
