package xdr.locallex.struct;

public class Stat_Event_Cell
{
    public int iCityID;
    public int iECI;
    public int iInterface;
    public int kpiSet;
    public int iTime;
	public double fvalue[] = new double[20];
	public static final String spliter = "\t";
    
	public static Stat_Event_Cell fillData(String[] vals, int pos){
		Stat_Event_Cell cell = new Stat_Event_Cell();
		try{
			cell.iCityID = Integer.parseInt(vals[pos++]);
			cell.iECI = Integer.parseInt(vals[pos++]);
			cell.iInterface = Integer.parseInt(vals[pos++]);
			cell.kpiSet = Integer.parseInt(vals[pos++]);
			cell.iTime = Integer.parseInt(vals[pos++]);
			for (int i = 0; i < cell.fvalue.length; i++)
			{
				cell.fvalue[i] = Double.parseDouble(vals[pos++]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return cell;
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(iCityID);
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
