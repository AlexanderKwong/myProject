package xdr.lablefill;


import StructData.Stat_CellGrid_4G;
import mergestat.IMergeDataDo;

public class CellGridMergeDataDo_4G implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_CellGrid_4G statItem = new Stat_CellGrid_4G();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.iCi);sbTemp.append("_");
		sbTemp.append(statItem.itllongitude);sbTemp.append("_");
		sbTemp.append(statItem.itllatitude);sbTemp.append("_");
		sbTemp.append(statItem.startTime);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		return dataType;
	}
	
	@Override
	public int setDataType(int dataType)
	{
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		CellGridMergeDataDo_4G tmpItem = (CellGridMergeDataDo_4G)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		statItem.iduration += tmpItem.statItem.iduration;
		statItem.idistance += tmpItem.statItem.idistance;
		statItem.isamplenum += tmpItem.statItem.isamplenum;

		// rsrp
		if (tmpItem.statItem.tStat.RSRP_nTotal > 0)
		{
			statItem.tStat.RSRP_nTotal += tmpItem.statItem.tStat.RSRP_nTotal;
			statItem.tStat.RSRP_nSum += tmpItem.statItem.tStat.RSRP_nSum;
		}

		for (int i = 0; i < statItem.tStat.RSRP_nCount.length; i++)
		{
			statItem.tStat.RSRP_nCount[i] += tmpItem.statItem.tStat.RSRP_nCount[i];
		}
		statItem.tStat.RSRP_nCount7 += tmpItem.statItem.tStat.RSRP_nCount7;

		//RSRQ
		if (tmpItem.statItem.tStat.RSRQ_nTotal > 0)
		{
			statItem.tStat.RSRQ_nTotal += tmpItem.statItem.tStat.RSRQ_nTotal;
			statItem.tStat.RSRQ_nSum += tmpItem.statItem.tStat.RSRQ_nSum;
		}

		for (int i = 0; i < tmpItem.statItem.tStat.RSRQ_nCount.length; i++)
		{
			statItem.tStat.RSRQ_nCount[i] += tmpItem.statItem.tStat.RSRQ_nCount[i];
		}
		
		// sinr
		if (tmpItem.statItem.tStat.SINR_nTotal > 0)
		{
			statItem.tStat.SINR_nTotal += tmpItem.statItem.tStat.SINR_nTotal;
			statItem.tStat.SINR_nSum += tmpItem.statItem.tStat.SINR_nSum;
		}

		for (int i = 0; i < statItem.tStat.SINR_nCount.length; i++)
		{
			statItem.tStat.SINR_nCount[i] += tmpItem.statItem.tStat.SINR_nCount[i];
		}

		statItem.tStat.RSRP100_SINR0 += tmpItem.statItem.tStat.RSRP100_SINR0;
		statItem.tStat.RSRP105_SINR0 += tmpItem.statItem.tStat.RSRP105_SINR0;
		statItem.tStat.RSRP110_SINR3 += tmpItem.statItem.tStat.RSRP110_SINR3;
		statItem.tStat.RSRP110_SINR0 += tmpItem.statItem.tStat.RSRP110_SINR0;

		statItem.tStat.UpLen += tmpItem.statItem.tStat.UpLen;
		statItem.tStat.DwLen += tmpItem.statItem.tStat.DwLen;
		statItem.tStat.DurationU += tmpItem.statItem.tStat.DurationU;
		statItem.tStat.DurationD += tmpItem.statItem.tStat.DurationD;

		//kbps
		if (statItem.tStat.DurationU > 0)
			statItem.tStat.AvgUpSpeed = (float) (statItem.tStat.UpLen / (statItem.tStat.DurationU / 1000.0)
					* 8.0) / 1024;
		statItem.tStat.MaxUpSpeed = Math.max(statItem.tStat.MaxUpSpeed, tmpItem.statItem.tStat.MaxUpSpeed);
		
		if (statItem.tStat.DurationD > 0)
			statItem.tStat.AvgDwSpeed = (float) (statItem.tStat.DwLen / (statItem.tStat.DurationD / 1000.0)
					* 8.0) / 1024;
		statItem.tStat.MaxDwSpeed = Math.max(statItem.tStat.MaxDwSpeed, tmpItem.statItem.tStat.MaxDwSpeed);

		statItem.tStat.UpLen_1M += tmpItem.statItem.tStat.UpLen_1M;
		statItem.tStat.DwLen_1M += tmpItem.statItem.tStat.DwLen_1M;
		statItem.tStat.DurationU_1M += tmpItem.statItem.tStat.DurationU_1M;
		statItem.tStat.DurationD_1M += tmpItem.statItem.tStat.DurationD_1M;

		if (statItem.tStat.DurationU_1M > 0)
			statItem.tStat.AvgUpSpeed_1M = (float) (statItem.tStat.UpLen_1M
					/ (statItem.tStat.DurationU_1M / 1000.0) * 8.0) / 1024;
		statItem.tStat.MaxUpSpeed_1M = Math.max(statItem.tStat.MaxUpSpeed_1M, tmpItem.statItem.tStat.MaxUpSpeed_1M);
		if (statItem.tStat.DurationD_1M > 0)
			statItem.tStat.AvgDwSpeed_1M = (float) (statItem.tStat.DwLen_1M
					/ (statItem.tStat.DurationD_1M / 1000.0) * 8.0) / 1024;
		statItem.tStat.MaxDwSpeed_1M = Math.max(statItem.tStat.MaxDwSpeed_1M, tmpItem.statItem.tStat.MaxDwSpeed_1M);
	
		statItem.UserCount_4G += tmpItem.statItem.UserCount_4G;
		statItem.UserCount_3G += tmpItem.statItem.UserCount_3G;
		statItem.UserCount_2G += tmpItem.statItem.UserCount_2G;
		statItem.UserCount_4GFall += tmpItem.statItem.UserCount_4GFall;
		statItem.XdrCount += tmpItem.statItem.XdrCount;
		statItem.MrCount += tmpItem.statItem.MrCount;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = Stat_CellGrid_4G.FillData(vals, 0);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutCellGrid_4G(statItem);
	}

	
	
	
}
