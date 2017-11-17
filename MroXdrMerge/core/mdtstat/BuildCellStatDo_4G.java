package mdtstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.struct.Stat_mdt_BuildCell;
import mrstat.AStatDo;
import mrstat.MrStatUtil;
import mrstat.TypeResult;

public class BuildCellStatDo_4G extends AStatDo
{
	private Map<String, Stat_mdt_BuildCell> high_buildcellMap;
	private Map<String, Stat_mdt_BuildCell> low_buildcellMap;
	private int sourceType;

	public BuildCellStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_buildcellMap = new HashMap<String, Stat_mdt_BuildCell>();// cityid
		low_buildcellMap = new HashMap<String, Stat_mdt_BuildCell>();
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
			String key = sample.cityID + "_" + sample.ispeed + "_" + sample.Eci + "_" + sample.imode + "_" + Util.RoundTimeForHour(sample.itime) + "_" + sample.mrType + "_" + ifreq;
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_mdt_BuildCell statBuild = high_buildcellMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_mdt_BuildCell();
					statBuild.doFirstSample(sample, ifreq);
					high_buildcellMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_mdt_BuildCell statBuild = low_buildcellMap.get(key);
				if (statBuild == null)
				{
					statBuild = new Stat_mdt_BuildCell();
					statBuild.doFirstSample(sample, ifreq);
					low_buildcellMap.put(key, statBuild);
				}
				deal(sample, statBuild);
			}
		}
		// }
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_mdt_BuildCell statBuild)
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
			for (Stat_mdt_BuildCell item : high_buildcellMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_BUILDCELL_HIGH, item.toLine());
			}
			for (Stat_mdt_BuildCell item : low_buildcellMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_BUILDCELL_LOW, item.toLine());
			}
		}
		high_buildcellMap.clear();
		low_buildcellMap.clear();
		return 0;
	}

}
