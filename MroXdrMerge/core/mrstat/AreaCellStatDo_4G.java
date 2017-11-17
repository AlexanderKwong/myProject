package mrstat;

import java.util.HashMap;
import java.util.Map;

import mdtstat.Util;
import mrstat.struct.Scene_Cell;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;

public class AreaCellStatDo_4G extends AStatDo
{

	private Map<String, Scene_Cell> sceneDataMap;
	private int sourceType;

	public AreaCellStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		sceneDataMap = new HashMap<String, Scene_Cell>();
	}

	@Override
	public int statSub(Object tsam)
	{
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (!MrStatUtil.rsrpRight(sample, sourceType))// 如果rsrp不合法 直接返回
		{
			return 0;
		}
		if (sample.iAreaType > 0 && sample.iAreaID > 0 && sample.testType == StaticConfig.TestType_HiRail)
		{
			String key = sample.cityID + "_" + sample.iAreaType + "_" + sample.iAreaID + "_" + sample.Eci + "_" + Util.RoundTimeForHour(sample.itime);
			Scene_Cell statScene = sceneDataMap.get(key);
			if (statScene == null)
			{
				statScene = new Scene_Cell();
				statScene.doFirstSample(sample);
				// TODO 一些赋值
				sceneDataMap.put(key, statScene);
			}
			deal(sample, statScene);
		}
		return 0;
	}
	
	private void deal(DT_Sample_4G sample, Scene_Cell statScene)
	{
//		if (sourceType == StaticConfig.SOURCE_YD)
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
		{
			statScene.doSample(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			statScene.doSampleLT(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			statScene.doSampleDX(sample);
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
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			for (Scene_Cell item : sceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_AREA_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Scene_Cell item : sceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_AREA_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Scene_Cell item : sceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_AREA_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Scene_Cell item : sceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_AREA_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Scene_Cell item : sceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_AREA_CELL, item.toLine());
			}
		}
		sceneDataMap.clear();
		return 0;
	}

}
