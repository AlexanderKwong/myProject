package mrstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Scene_Grid;
import mrstat.struct.Stat_Scene;

public class AreaStatDo_4G extends AStatDo
{
	private Map<String, Stat_Scene> SceneDataMap;
	private int sourceType;

	public AreaStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		// TODO Auto-generated constructor stub
		this.sourceType = sourceType;
		SceneDataMap = new HashMap<String, Stat_Scene>();
	}

	@Override
	public int statSub(Object tsam)
	{
		// TODO Auto-generated method stub
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (!MrStatUtil.rsrpRight(sample, sourceType))// 如果rsrp不合法 直接返回
		{
			return 0;
		}
		if (sample.iAreaType > 0 && sample.iAreaID > 0 && sample.testType == StaticConfig.TestType_HiRail)
		{
			String key = sample.cityID + "_" + sample.iAreaType + "_" + sample.iAreaID + "_" + Util.RoundTimeForHour(sample.itime);
			Stat_Scene statScene = SceneDataMap.get(key);
			if (statScene == null)
			{
				statScene = new Stat_Scene();
				statScene.doFirstSample(sample);
				// TODO 一些赋值
				SceneDataMap.put(key, statScene);
			}
			deal(sample, statScene);
		}
		return 0;
	}
	
	private void deal(DT_Sample_4G sample, Stat_Scene statScene)
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
		// TODO Auto-generated method stub
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			for (Stat_Scene item : SceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_AREA, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Stat_Scene item : SceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_AREA, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_Scene item : SceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_AREA, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Stat_Scene item : SceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_AREA, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Stat_Scene item : SceneDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_AREA, item.toLine());
			}
		}
		SceneDataMap.clear();
		return 0;
	}

}
