package mrstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Stat_Build;

public class BuildStatDo_4G extends AStatDo
{
	private Map<String, Stat_Build> high_buildMap;
	private Map<String, Stat_Build> mid_buildMap;
	private Map<String, Stat_Build> low_buildMap;
	private int sourceType;

	public BuildStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_buildMap = new HashMap<String, Stat_Build>();// cityid
		mid_buildMap = new HashMap<String, Stat_Build>(); // +longtitude+latitude作为key
		low_buildMap = new HashMap<String, Stat_Build>();
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
		if (sample.Eci > 0 && sample.ispeed >= 0)
		{
			String key = sample.cityID + "_" + sample.ispeed + "_" + sample.imode + "_" + ifreq + "_" + Util.RoundTimeForHour(sample.itime);
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_Build statBuild = high_buildMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_Build();
					statBuild.doFirstSample(sample, ifreq);
					high_buildMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
			else if (sample.ConfidenceType == StaticConfig.IM)
			{
				Stat_Build statBuild = mid_buildMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_Build();
					statBuild.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					mid_buildMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_Build statBuild = low_buildMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_Build();
					statBuild.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					low_buildMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
		}
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_Build statBuild)
	{
		// if (sourceType == StaticConfig.SOURCE_YD)
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
		{
			statBuild.doSample(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			statBuild.doSampleDX(sample);
		}
		else
		{
			statBuild.doSampleLT(sample);
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
			for (Stat_Build item : high_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_HIGH_BUILD, item.toLine());
			}
			for (Stat_Build item : mid_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_MID_BUILD, item.toLine());
			}
			for (Stat_Build item : low_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LOW_BUILD, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_Build item : high_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_HIGH_BUILD, item.toLine());
			}
			for (Stat_Build item : mid_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_MID_BUILD, item.toLine());
			}
			for (Stat_Build item : low_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_LOW_BUILD, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Stat_Build item : high_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_HIGH_BUILD, item.toLine());
			}
			for (Stat_Build item : mid_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_MID_BUILD, item.toLine());
			}
			for (Stat_Build item : low_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_LOW_BUILD, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Stat_Build item : high_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_HIGH_BUILD, item.toLine());
			}
			for (Stat_Build item : mid_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_MID_BUILD, item.toLine());
			}
			for (Stat_Build item : low_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_LOW_BUILD, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Stat_Build item : high_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_HIGH_BUILD, item.toLine());
			}
			for (Stat_Build item : mid_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_MID_BUILD, item.toLine());
			}
			for (Stat_Build item : low_buildMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_LOW_BUILD, item.toLine());
			}
		}
		high_buildMap.clear();
		mid_buildMap.clear();
		low_buildMap.clear();
		return 0;
	}
}
