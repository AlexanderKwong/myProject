package mrstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.Util;
import mrstat.struct.Stat_Cell;

/**
 * Created by Administrator on 2017/5/8.
 */
public class CellStatDO_4G extends AStatDo
{

	private Map<String, Stat_Cell> cellDataMap;
	private int sourceType;

	public CellStatDO_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		cellDataMap = new HashMap<String, Stat_Cell>();
	}

	@Override
	public int statSub(Object tsam)
	{
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (sample.Eci > 0)
		{
			if (!MrStatUtil.rsrpRight(sample, sourceType))// 如果rsrp不合法 直接返回
			{
				return 0;
			}
			int ifreq = getIfreq(sourceType, sample);
			String key = sample.cityID + "_" + sample.Eci + "_" + ifreq + "_" + Util.RoundTimeForHour(sample.itime);
			Stat_Cell statCell = cellDataMap.get(key);
			if (statCell == null)
			{
				statCell = new Stat_Cell();
				statCell.doFirstSample(sample, ifreq);
				// TODO 一些赋值
				cellDataMap.put(key, statCell);
			}
			deal(sample, statCell);
		}
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_Cell statCell)
	{
//		if (sourceType == StaticConfig.SOURCE_YD)
		if (sourceType == StaticConfig.SOURCE_YD || sourceType == StaticConfig.SOURCE_YDLT || sourceType == StaticConfig.SOURCE_YDDX)
		{
			statCell.doSample(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			statCell.doSampleDX(sample);
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			statCell.doSampleLT(sample);
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
			for (Stat_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_DX_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_LT)
		{
			for (Stat_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_LT_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDLT)
		{
			for (Stat_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDLT_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_YDDX)
		{
			for (Stat_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MroNewTableStat.DataType_YDDX_CELL, item.toLine());
			}
		}
		cellDataMap.clear();
		return 0;
	}
}
