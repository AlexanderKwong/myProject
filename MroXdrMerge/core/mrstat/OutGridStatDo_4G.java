package mrstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Stat_OutGrid;

public class OutGridStatDo_4G extends AStatDo
{
	private Map<String, Stat_OutGrid> high_out_gridDataMap;
	private Map<String, Stat_OutGrid> mid_out_gridDataMap;
	private Map<String, Stat_OutGrid> low_out_gridDataMap;
	private int sourceType;

	public OutGridStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_out_gridDataMap = new HashMap<String, Stat_OutGrid>(); // cityid_longtitude_latitude作为key
		mid_out_gridDataMap = new HashMap<String, Stat_OutGrid>();
		low_out_gridDataMap = new HashMap<String, Stat_OutGrid>();
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
		if (sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			String key = sample.cityID + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_" + ifreq + "_" + Util.RoundTimeForHour(sample.itime);
			int locSource = sample.locSource;// 经纬度来源
			if (sample.ConfidenceType == StaticConfig.OH)
			{
				Stat_OutGrid outGrid = high_out_gridDataMap.get(key);
				if (outGrid == null)
				{
					outGrid = new Stat_OutGrid();
					outGrid.doFirstSample(sample, ifreq);
					high_out_gridDataMap.put(key, outGrid);
				}
				deal(sample, outGrid);
			}
			else if (sample.ConfidenceType == StaticConfig.OM)
			{
				Stat_OutGrid outGrid = mid_out_gridDataMap.get(key);
				if (outGrid == null)
				{
					outGrid = new Stat_OutGrid();
					outGrid.doFirstSample(sample, ifreq);
					mid_out_gridDataMap.put(key, outGrid);
				}
				deal(sample, outGrid);
			}
			else if (sample.ConfidenceType == StaticConfig.OL)
			{
				Stat_OutGrid outGrid = low_out_gridDataMap.get(key);
				if (outGrid == null)
				{
					outGrid = new Stat_OutGrid();
					outGrid.doFirstSample(sample, ifreq);
					low_out_gridDataMap.put(key, outGrid);
				}
				deal(sample, outGrid);
			}
		}
		// }
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_OutGrid outGrid)
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
			for (Stat_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_HIGH_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : mid_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_MID_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LOW_OUT_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Stat_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_HIGH_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : mid_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_MID_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_LOW_OUT_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_HIGH_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : mid_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_MID_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_LOW_OUT_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Stat_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_HIGH_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : mid_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_MID_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_LOW_OUT_GRID, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Stat_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_HIGH_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : mid_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_MID_OUT_GRID, item.toLine());
			}
			for (Stat_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_LOW_OUT_GRID, item.toLine());
			}
		}
		high_out_gridDataMap.clear();
		mid_out_gridDataMap.clear();
		low_out_gridDataMap.clear();
		return 0;
	}

}
