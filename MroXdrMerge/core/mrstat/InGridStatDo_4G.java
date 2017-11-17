package mrstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Stat_InGrid;

public class InGridStatDo_4G extends AStatDo
{

	private Map<String, Stat_InGrid> high_in_gridDataMap;
	private Map<String, Stat_InGrid> mid_in_gridDataMap;
	private Map<String, Stat_InGrid> low_in_gridDataMap;
	private int sourceType;

	public InGridStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_in_gridDataMap = new HashMap<String, Stat_InGrid>();
		mid_in_gridDataMap = new HashMap<String, Stat_InGrid>();
		low_in_gridDataMap = new HashMap<String, Stat_InGrid>();
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
			String key = sample.cityID + "_" + sample.ispeed + "_" + sample.imode + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_" + ifreq + "_"
					+ Util.RoundTimeForHour(sample.itime);
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				Stat_InGrid statInGrid = high_in_gridDataMap.get(key);
				if (statInGrid == null)
				{
					statInGrid = new Stat_InGrid();
					statInGrid.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					high_in_gridDataMap.put(key, statInGrid);
				}
				deal(sample, statInGrid);
			}
			else if (sample.ConfidenceType == StaticConfig.IM)
			{
				Stat_InGrid statInGrid = mid_in_gridDataMap.get(key);
				if (statInGrid == null)
				{
					statInGrid = new Stat_InGrid();
					statInGrid.doFirstSample(sample, ifreq);
					// TODO 一些赋值
					mid_in_gridDataMap.put(key, statInGrid);
				}
				deal(sample, statInGrid);
			}
			else if (sample.ConfidenceType == StaticConfig.IL)
			{
				Stat_InGrid statInGrid = low_in_gridDataMap.get(key);
				if (statInGrid == null)
				{
					statInGrid = new Stat_InGrid();
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

	private void deal(DT_Sample_4G sample, Stat_InGrid statInGrid)
	{
//		if (sourceType == StaticConfig.SOURCE_YD)
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
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
			for (Stat_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_HIGH_IN_GRID, item.toLine());
			}

			for (Stat_InGrid item : mid_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_MID_IN_GRID, item.toLine());
			}

			for (Stat_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LOW_IN_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_HIGH_IN_GRID, item.toLine());
			}

			for (Stat_InGrid item : mid_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_MID_IN_GRID, item.toLine());
			}

			for (Stat_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_LOW_IN_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Stat_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_HIGH_IN_GRID, item.toLine());
			}

			for (Stat_InGrid item : mid_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_MID_IN_GRID, item.toLine());
			}

			for (Stat_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_LOW_IN_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Stat_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_HIGH_IN_GRID, item.toLine());
			}
			for (Stat_InGrid item : mid_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_MID_IN_GRID, item.toLine());
			}
			for (Stat_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_LOW_IN_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Stat_InGrid item : high_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_HIGH_IN_GRID, item.toLine());
			}
			for (Stat_InGrid item : mid_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_MID_IN_GRID, item.toLine());
			}
			for (Stat_InGrid item : low_in_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_LOW_IN_GRID, item.toLine());
			}
		}
		high_in_gridDataMap.clear();
		mid_in_gridDataMap.clear();
		low_in_gridDataMap.clear();
		return 0;
	}
}
