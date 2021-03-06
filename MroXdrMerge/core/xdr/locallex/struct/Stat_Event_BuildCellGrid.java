package xdr.locallex.struct;

public class Stat_Event_BuildCellGrid
{
    public int iCityID;
	protected int iBuildingID;
    public int iECI;
    public int iInterface;
    public int kpiSet;
    public int iTime;
	public double[] fvalue = new double[20];
	public static final String spliter = "\t";
    
	
	public static Stat_Event_BuildCellGrid fillData(String[] vals, int pos){
		Stat_Event_BuildCellGrid buildCellGrid = new Stat_Event_BuildCellGrid();
		try{
			buildCellGrid.iCityID = Integer.parseInt(vals[pos++]);
			buildCellGrid.iBuildingID = Integer.parseInt(vals[pos++]);
			buildCellGrid.iECI = Integer.parseInt(vals[pos++]);
			buildCellGrid.iInterface = Integer.parseInt(vals[pos++]);
			buildCellGrid.kpiSet = Integer.parseInt(vals[pos++]);
			buildCellGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < buildCellGrid.fvalue.length; i++)
			{
				buildCellGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return buildCellGrid;
	}
	
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
		sb.append(spliter);
		sb.append(iBuildingID);
		sb.append(spliter);
		sb.append(iECI);
		sb.append(spliter);
		sb.append(iInterface);
		sb.append(spliter);
		sb.append(kpiSet);
		sb.append(spliter);
		sb.append(iTime);
		sb.append(spliter);
		for (int i = 0; i < fvalue.length; i++)
		{
			sb.append(fvalue[i]);
			if(i!=fvalue.length-1){
				sb.append(spliter);
			}
		}	
	
		return sb.toString();
	}

}
