package xdr.locallex.struct;

import mergestat.IMergeDataDo;

public class EventBuildCellGrid_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_Event_BuildCellGrid buildCellGrid = new Stat_Event_BuildCellGrid();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildCellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(buildCellGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildCellGrid.iInterface);
		sbTemp.append("_");
		sbTemp.append(buildCellGrid.kpiSet);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		// TODO Auto-generated method stub
		EventBuildCellGrid_mergeDo data = (EventBuildCellGrid_mergeDo) o;
		for (int i = 0; i < data.buildCellGrid.fvalue.length; i++)
		{
			if(data.buildCellGrid.fvalue[i] > 0)
			{
				if(buildCellGrid.fvalue[i] < 0){
					buildCellGrid.fvalue[i] = data.buildCellGrid.fvalue[i];
				}else{
					buildCellGrid.fvalue[i] += data.buildCellGrid.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		buildCellGrid = Stat_Event_BuildCellGrid.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return buildCellGrid.toString();
	}

}
