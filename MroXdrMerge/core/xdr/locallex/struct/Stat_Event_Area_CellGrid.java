package xdr.locallex.struct;

public class Stat_Event_Area_CellGrid
{
    public int iCityID;
    public int iTLlongitude;
    public int iTLlatitude;
    public int iBRlongitude;
    public int iBRlatitude;
    public int iECI;
    public int iInterface;
    public int kpiSet;
    public int iTime;
    public int iAreatype;
    public int iAreaID;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
    
	public static Stat_Event_Area_CellGrid fillData(String[] vals, int pos){
		Stat_Event_Area_CellGrid areaCellGrid = new Stat_Event_Area_CellGrid();
		try{
			areaCellGrid.iCityID = Integer.parseInt(vals[pos++]);
			areaCellGrid.iAreatype = Integer.parseInt(vals[pos++]);
			areaCellGrid.iAreaID = Integer.parseInt(vals[pos++]);
			areaCellGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iTLlatitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			areaCellGrid.iECI = Integer.parseInt(vals[pos++]);
			areaCellGrid.iInterface = Integer.parseInt(vals[pos++]);
			areaCellGrid.kpiSet = Integer.parseInt(vals[pos++]);
			areaCellGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < areaCellGrid.fvalue.length; i++)
			{
				areaCellGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return areaCellGrid;
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
		sb.append(spliter);
		sb.append(iAreatype);
		sb.append(spliter);
		sb.append(iAreaID);
		sb.append(spliter);
		sb.append(iTLlongitude);
		sb.append(spliter);
		sb.append(iTLlatitude);
		sb.append(spliter);
		sb.append(iBRlongitude);
		sb.append(spliter);
		sb.append(iBRlatitude);
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
