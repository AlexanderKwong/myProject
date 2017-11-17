package mdtstat;

import java.util.HashMap;
import java.util.Map;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mrstat.AStatDo;
import mrstat.MrStatUtil;
import mrstat.TypeResult;
import mdtstat.struct.Stat_mdt_Build;
import mdtstat.Util;

public class BuildStatDo_4G extends AStatDo
{
	private Map<String, Stat_mdt_Build> high_buildMap;
	private Map<String, Stat_mdt_Build> low_buildMap;
	private int sourceType;

	public BuildStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_buildMap = new HashMap<String, Stat_mdt_Build>();// cityid
		low_buildMap = new HashMap<String, Stat_mdt_Build>();
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
		if (sample.Eci > 0)
		{
			String key = sample.cityID + "_" + sample.ispeed + "_" + sample.imode + "_" + Util.RoundTimeForHour(sample.itime) + "_" + sample.mrType + "_" + ifreq;
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_mdt_Build statBuild = high_buildMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_mdt_Build();
					statBuild.doFirstSample(sample, ifreq);
					high_buildMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_mdt_Build statBuild = low_buildMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_mdt_Build();
					statBuild.doFirstSample(sample, ifreq);
					low_buildMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
		}
		// }
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_mdt_Build statBuild)
	{
		if (sourceType == StaticConfig.SOURCE_YD)
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
			for (Stat_mdt_Build item : high_buildMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_BUILD_HIGH, item.toLine());
			}
			for (Stat_mdt_Build item : low_buildMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_BUILD_LOW, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_mdt_Build item : high_buildMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_BUILD_HIGH, item.toLine());
			}
			for (Stat_mdt_Build item : low_buildMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_BUILD_LOW, item.toLine());
			}
		}
		else
		{
			for (Stat_mdt_Build item : high_buildMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_BUILD_HIGH, item.toLine());
			}
			for (Stat_mdt_Build item : low_buildMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_BUILD_LOW, item.toLine());
			}
		}
		high_buildMap.clear();
		low_buildMap.clear();
		return 0;
	}
}
