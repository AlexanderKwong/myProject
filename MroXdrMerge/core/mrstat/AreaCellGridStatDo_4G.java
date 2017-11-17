package mrstat;

import java.util.HashMap;
import java.util.Map;

import mdtstat.Util;
import mrstat.struct.Scene_CellGrid;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;

public class AreaCellGridStatDo_4G extends AStatDo
{
	private Map<String, Scene_CellGrid> scene_cellgridDataMap;
	private int sourceType;

	public AreaCellGridStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		scene_cellgridDataMap = new HashMap<String, Scene_CellGrid>();
	}

	@Override
	public int statSub(Object tsam)
	{
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (!MrStatUtil.rsrpRight(sample, sourceType))// 如果rsrp不合法 直接返回
		{
			return 0;
		}

		if (sample.testType == StaticConfig.TestType_HiRail && sample.Eci > 0 && sample.iAreaType > 0 && sample.iAreaID > 0 && sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			String key = sample.cityID + "_" + sample.iAreaType + "_" + sample.iAreaID + "_" + sample.Eci + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_" + Util.RoundTimeForHour(sample.itime);
			Scene_CellGrid outCellGrid = scene_cellgridDataMap.get(key);
			if (outCellGrid == null)
			{
				outCellGrid = new Scene_CellGrid();
				outCellGrid.doFirstSample(sample);
				scene_cellgridDataMap.put(key, outCellGrid);
			}
			outCellGrid.doSample(sample);
		}
		return 0;
	}

	// /**
	// * 专门统计iASNei_MRCnt fASNei_RSRPValue
	// *
	// * @param sample
	// */
	// public void doNCStatisitc(DT_Sample_4G sample, Map<String,
	// Scene_CellGrid> scene_cellgridDataMap)
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
	// Scene_CellGrid outCellGrid = scene_cellgridDataMap.get(ncKey);
	// if (outCellGrid == null)
	// {
	// outCellGrid = new Scene_CellGrid();
	// outCellGrid.doFirstSample(sample);
	// scene_cellgridDataMap.put(ncKey, outCellGrid);
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
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			for (Scene_CellGrid item : scene_cellgridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_AREA_CELL_GRID, item.toLine());
			}
		}
		scene_cellgridDataMap.clear();
		return 0;
	}

}
