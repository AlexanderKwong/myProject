package mdtstat;

import java.util.HashMap;
import java.util.Map;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.struct.Stat_mdt_InGrid;
import mrstat.AStatDo;
import mrstat.MrStatUtil;
import mrstat.TypeResult;

public class InGridStatDo_4G extends AStatDo
{

	private Map<String, Stat_mdt_InGrid> high_in_gridDataMap;
	private Map<String, Stat_mdt_InGrid> low_in_gridDataMap;
	private int sourceType;

	public InGridStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_in_gridDataMap = new HashMap<String, Stat_mdt_InGrid>();
		low_in_gridDataMap = new HashMap<String, Stat_mdt_InGrid>();
	}

	@Override
	public int statSub(Object tsam)
	{
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (!MrStatUtil.rsrpRight(sample, sourceType))// 如果rsrp不合法 直接返回
		{
			return 0;
		}
		int ifreq = getIfreq(sourceType, sample);
		// if (sample.samState == StaticConfig.ACTTYPE_IN)
		// {
		if (sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			String key = sample.cityID + "_" + Util.RoundTimeForHour(sample.itime) + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_" + sample.ispeed + "_"
					+ sample.imode + "_" + ifreq + "_" + sample.mrType;
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_mdt_InGrid statInGrid = high_in_gridDataMap.get(key);
				if (statInGrid == null)
				{
					statInGrid = new Stat_mdt_InGrid();
					statInGrid.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					high_in_gridDataMap.put(key, statInGrid);
				}
				deal(sample, statInGrid);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_mdt_InGrid statInGrid = low_in_gridDataMap.get(key);
				if (statInGrid == null)
				{
					statInGrid = new Stat_mdt_InGrid();
					statInGrid.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					low_in_gridDataMap.put(key, statInGrid);
				}
				deal(sample, statInGrid);
			}
		}
		// }
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_mdt_InGrid statInGrid)
	{
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			statInGrid.doSample(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			statInGrid.doSampleDX(sample);
		}
		else
		{
			statInGrid.doSampleLT(sample);
		}
	}

	@Override
	public int outDealingResultSub()
	{
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		if (sourceType == StaticConfig.SOURCE_YD)
		{
			for (Stat_mdt_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_INGRID_HIGH, item.toLine());
			}

			for (Stat_mdt_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_INGRID_LOW, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_mdt_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_INGRID_HIGH, item.toLine());
			}

			for (Stat_mdt_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_INGRID_LOW, item.toLine());
			}
		}
		else
		{
			for (Stat_mdt_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_INGRID_HIGH, item.toLine());
			}

			for (Stat_mdt_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_INGRID_LOW, item.toLine());
			}
		}
		high_in_gridDataMap.clear();
		low_in_gridDataMap.clear();
		return 0;
	}
}
