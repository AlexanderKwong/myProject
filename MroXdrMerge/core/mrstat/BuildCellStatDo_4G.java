package mrstat;

import java.util.HashMap;
import java.util.Map;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Stat_BuildCell;

public class BuildCellStatDo_4G extends AStatDo
{
	private Map<String, Stat_BuildCell> high_buildCellMap;
	private Map<String, Stat_BuildCell> mid_buildCellMap;
	private Map<String, Stat_BuildCell> low_buildCellMap;
	private int sourceType;

	public BuildCellStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_buildCellMap = new HashMap<String, Stat_BuildCell>();// cityid
		mid_buildCellMap = new HashMap<String, Stat_BuildCell>(); // +longtitude+latitude作为key
		low_buildCellMap = new HashMap<String, Stat_BuildCell>();
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
		int ifreq = getIfreq(sourceType, sample);
		// if (sample.samState == StaticConfig.ACTTYPE_IN)
		// {
		if (sample.Eci > 0 && sample.ispeed >= 0)
		{
			String key = sample.cityID + "_" + sample.ispeed + "_" + sample.imode + "_" + sample.Eci + "_" + ifreq + "_" + Util.RoundTimeForHour(sample.itime);
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_BuildCell statBuild = high_buildCellMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_BuildCell();
					statBuild.doFirstSample(sample, ifreq);
					high_buildCellMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
			else if (sample.ConfidenceType == StaticConfig.IM)
			{
				Stat_BuildCell statBuild = mid_buildCellMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_BuildCell();
					statBuild.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					mid_buildCellMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_BuildCell statBuild = low_buildCellMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_BuildCell();
					statBuild.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					low_buildCellMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
		}
		// }
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_BuildCell statBuild)
	{
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			statBuild.doSample(sample);
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
			for (Stat_BuildCell item : high_buildCellMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_HIGH_BUILDCELL, item.toLine());
			}
			for (Stat_BuildCell item : mid_buildCellMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_MID_BUILDCELL, item.toLine());
			}
			for (Stat_BuildCell item : low_buildCellMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LOW_BUILDCELL, item.toLine());
			}
		}
		high_buildCellMap.clear();
		mid_buildCellMap.clear();
		low_buildCellMap.clear();
		return 0;
	}

}
