package mdtstat;

import java.util.HashMap;
import java.util.Map;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mdtstat.struct.Stat_mdt_Cell;
import mrstat.AStatDo;
import mrstat.MrStatUtil;
import mrstat.TypeResult;

public class CellStatDO_4G extends AStatDo
{
	private Map<String, Stat_mdt_Cell> cellDataMap;
	private int sourceType;

	public CellStatDO_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		cellDataMap = new HashMap<String, Stat_mdt_Cell>();
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
			Stat_mdt_Cell statCell = cellDataMap.get(key);
			if (statCell == null)
			{
				statCell = new Stat_mdt_Cell();
				statCell.doFirstSample(sample, ifreq);
				cellDataMap.put(key, statCell);
			}
			deal(sample, statCell);
		}
		return 0;
	}

	private void deal(DT_Sample_4G sample, Stat_mdt_Cell statCell)
	{
		if (sourceType == StaticConfig.SOURCE_YD)
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
			for (Stat_mdt_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_MDTMR_CELL, item.toLine());
			}
		}
		else if (sourceType == StaticConfig.SOURCE_DX)
		{
			for (Stat_mdt_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_DX_MDTMR_CELL, item.toLine());
			}
		}
		else
		{
			for (Stat_mdt_Cell item : cellDataMap.values())
			{
				typeResult.pushData(MdtNewTableStat.DataType_TB_LT_MDTMR_CELL, item.toLine());
			}
		}
		cellDataMap.clear();
		return 0;
	}
}
