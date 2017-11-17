package xdr.locallex.struct;

public class Stat_Event_OutCellGrid
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
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_OutCellGrid fillData(String[] vals, int pos){
		Stat_Event_OutCellGrid outCellGrid = new Stat_Event_OutCellGrid();
		try{
			outCellGrid.iCityID = Integer.parseInt(vals[pos++]);
			outCellGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			outCellGrid.iTLlatitude = Integer.parseInt(vals[pos++]);		
			outCellGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			outCellGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			outCellGrid.iECI = Integer.parseInt(vals[pos++]);
			outCellGrid.iInterface = Integer.parseInt(vals[pos++]);
			outCellGrid.kpiSet = Integer.parseInt(vals[pos++]);
			outCellGrid.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < outCellGrid.fvalue.length; i++)
			{
				outCellGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return outCellGrid;
	}
	
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
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
