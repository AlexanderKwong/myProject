package mrstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Scene_Grid;
import mrstat.struct.Stat_OutGrid;

public class AreaGridStatDo_4G extends AStatDo
{
	public Map<String, Scene_Grid> scene_gridDataMap;
	private int sourceType;

	public AreaGridStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		scene_gridDataMap = new HashMap<String, Scene_Grid>();
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
			String key = sample.cityID + "_" + sample.iAreaType + "_" + sample.iAreaID + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_"
					+ Util.RoundTimeForHour(sample.itime);
			Scene_Grid outGrid = scene_gridDataMap.get(key);
			if (outGrid == null)
			{
				outGrid = new Scene_Grid();
				outGrid.doFirstSample(sample);
				scene_gridDataMap.put(key, outGrid);
			}
			deal(sample, outGrid);
		}
		return 0;
	}
	
	private void deal(DT_Sample_4G sample, Scene_Grid outGrid)
	{
//		if (sourceType == StaticConfig.SOURCE_YD)
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
		{
			outGrid.doSample(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			outGrid.doSampleLT(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			outGrid.doSampleDX(sample);
		}
	}

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
			for (Scene_Grid item : scene_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_AREA_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Scene_Grid item : scene_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_AREA_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Scene_Grid item : scene_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_AREA_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Scene_Grid item : scene_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_AREA_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Scene_Grid item : scene_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_AREA_GRID, item.toLine());
			}
		}
		scene_gridDataMap.clear();
		return 0;

	}

}
