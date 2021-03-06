package xdr.locallex.struct;

public class Stat_Event_OutGrid
{
    public int iCityID;
    public int iTLlongitude;
    public int iTLlatitude;
    public int iTime;
    public int iBRlongitude;
    public int iBRlatitude;
    public int iInterface;
    public int kpiSet;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
	
	public static Stat_Event_OutGrid fillData(String[] vals, int pos){
		Stat_Event_OutGrid outGrid = new Stat_Event_OutGrid();
		try{
			outGrid.iCityID = Integer.parseInt(vals[pos++]);
			outGrid.iTLlongitude = Integer.parseInt(vals[pos++]);
			outGrid.iTLlatitude = Integer.parseInt(vals[pos++]);
			outGrid.iTime = Integer.parseInt(vals[pos++]);
			outGrid.iBRlongitude = Integer.parseInt(vals[pos++]);
			outGrid.iBRlatitude = Integer.parseInt(vals[pos++]);
			outGrid.iInterface = Integer.parseInt(vals[pos++]);
			outGrid.kpiSet = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < outGrid.fvalue.length; i++)
			{
				outGrid.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return outGrid;
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
