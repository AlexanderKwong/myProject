package xdr.lablefill.by23g;


import StructData.Stat_Cell_3G;
import mergestat.IMergeDataDo;
import xdr.lablefill.ResultHelper;

public class CellMergeDataDo_3G implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_Cell_3G statItem = new Stat_Cell_3G();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.iLAC);sbTemp.append("_");
		sbTemp.append(statItem.iCI);sbTemp.append("_");
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
		CellMergeDataDo_3G tmpItem = (CellMergeDataDo_3G)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		statItem.iduration += tmpItem.statItem.iduration;
		statItem.idistance += tmpItem.statItem.idistance;
		statItem.isamplenum += tmpItem.statItem.isamplenum;
		
		statItem.xdrCount += tmpItem.statItem.xdrCount;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = new Stat_Cell_3G();
		statItem.icityid = Integer.parseInt(vals[sPos++]);
		statItem.iLAC = Integer.parseInt(vals[sPos++]);
		statItem.iCI = Long.parseLong(vals[sPos++]);
		statItem.startTime = Integer.parseInt(vals[sPos++]);
		statItem.endTime = Integer.parseInt(vals[sPos++]);
		statItem.iduration = Integer.parseInt(vals[sPos++]);
		statItem.idistance = Integer.parseInt(vals[sPos++]);
		statItem.isamplenum = Integer.parseInt(vals[sPos++]);
		statItem.xdrCount = Integer.parseInt(vals[sPos++]);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutCell_3G(statItem);
	}

	
	
	
}
