package mdtstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.struct.Stat_mdt_OutGrid;
import mrstat.AStatDo;
import mrstat.MrStatUtil;
import mrstat.TypeResult;

public class OutGridStatDo_4G extends AStatDo
{
	private Map<String, Stat_mdt_OutGrid> high_out_gridDataMap;
	private Map<String, Stat_mdt_OutGrid> low_out_gridDataMap;
	private int sourceType;

	public OutGridStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		high_out_gridDataMap = new HashMap<String, Stat_mdt_OutGrid>(); // cityid_longtitude_latitude作为key
		low_out_gridDataMap = new HashMap<String, Stat_mdt_OutGrid>();
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
		// if (sample.samState == StaticConfig.ACTTYPE_OUT)
		// {
		if (sample.Eci > 0 && sample.ilongitude > 0 && sample.ilatitude > 0)
		{
			String key = sample.cityID + "_" + Util.RoundTimeForHour(sample.itime) + "_" + sample.grid.tllongitude + "_" + sample.grid.tllatitude + "_" + ifreq + "_"
					+ sample.mrType;
			int locSource = sample.locSource;// 经纬度来源
			if (sample.ConfidenceType == StaticConfig.OH)
			{
				Stat_mdt_OutGrid outGrid = high_out_gridDataMap.get(key);
				if (outGrid == null)
				{
					outGrid = new Stat_mdt_OutGrid();
					outGrid.doFirstSample(sample, ifreq);
					high_out_gridDataMap.put(key, outGrid);
				}
				deal(sample, outGrid);
			}
			else if (sample.ConfidenceType == StaticConfig.OL)
			{
				Stat_mdt_OutGrid outGrid = low_out_gridDataMap.get(key);
				if (outGrid == null)
				{
					outGrid = new Stat_mdt_OutGrid();
					outGrid.doFirstSample(sample, ifreq);
					low_out_gridDataMap.put(key, outGrid);
				}
				deal(sample, outGrid);
			}
		}
		// }
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_mdt_OutGrid outGrid)
	{
		if (sourceType == StaticConfig.SOURCE_YD)
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
			for (Stat_mdt_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_OUTGRID_HIGH, item.toLine());
			}
			for (Stat_mdt_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_OUTGRID_LOW, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Stat_mdt_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_OUTGRID_HIGH, item.toLine());
			}
			for (Stat_mdt_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_OUTGRID_LOW, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_mdt_OutGrid item : high_out_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_OUTGRID_HIGH, item.toLine());
			}
			for (Stat_mdt_OutGrid item : low_out_gridDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_OUTGRID_LOW, item.toLine());
			}
		}
		high_out_gridDataMap.clear();
		low_out_gridDataMap.clear();
		return 0;
	}

}
