package xdr.lablefill.by23g;



import StructData.Stat_CellGrid_2G;
import mergestat.IMergeDataDo;
import xdr.lablefill.ResultHelper;

public class CellGridMergeDataDo_2G implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_CellGrid_2G statItem = new Stat_CellGrid_2G();

	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(statItem.icityid);sbTemp.append("_");
		sbTemp.append(statItem.iLac);sbTemp.append("_");
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
		CellGridMergeDataDo_2G tmpItem = (CellGridMergeDataDo_2G)o;
		if(tmpItem == null)
		{
			return false;
		}
		
		statItem.iduration += tmpItem.statItem.iduration;
		statItem.idistance += tmpItem.statItem.idistance;
		statItem.isamplenum += tmpItem.statItem.isamplenum;
		
		statItem.UserCount += tmpItem.statItem.UserCount;
		statItem.XdrCount += tmpItem.statItem.XdrCount;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		sPos = 0;
		statItem = new Stat_CellGrid_2G();
		statItem.icityid = Integer.parseInt(vals[sPos++]);
		statItem.iLac = Integer.parseInt(vals[sPos++]);
		statItem.iCi = Integer.parseInt(vals[sPos++]);
		statItem.startTime = Integer.parseInt(vals[sPos++]);
		statItem.endTime = Integer.parseInt(vals[sPos++]);
		statItem.iduration = Integer.parseInt(vals[sPos++]);
		statItem.idistance = Integer.parseInt(vals[sPos++]);
		statItem.isamplenum = Integer.parseInt(vals[sPos++]);
		statItem.itllongitude = Integer.parseInt(vals[sPos++]);
		statItem.itllatitude = Integer.parseInt(vals[sPos++]);
		statItem.ibrlongitude = Integer.parseInt(vals[sPos++]);
		statItem.ibrlatitude = Integer.parseInt(vals[sPos++]);
		statItem.UserCount = Integer.parseInt(vals[sPos++]);
		statItem.XdrCount = Integer.parseInt(vals[sPos++]);
		
		return true;
	}

	@Override
	public String getData()
	{	
		return ResultHelper.getPutCellGrid_2G(statItem);
	}



	
	
	
}
