package xdr.locallex.struct;

public class Stat_Event_BuildGrid
{
    public int iCityID;
	protected int iBuildingID;
    public int iInterface;
    public int kpiSet;
    public int iTime;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_BuildGrid fillData(String[] vals, int pos){
		Stat_Event_BuildGrid buildGrid = new Stat_Event_BuildGrid();
		try{
			buildGrid.iCityID = Integer.parseInt(vals[pos++]);
			buildGrid.iBuildingID = Integer.parseInt(vals[pos++]);
			buildGrid.iInterface = Integer.parseInt(vals[pos++]);
			buildGrid.kpiSet = Integer.parseInt(vals[pos++]);
			buildGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < buildGrid.fvalue.length; i++)
			{
				buildGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return buildGrid;
	}
	
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
		sb.append(spliter);
		sb.append(iBuildingID);
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
