package mro.newformat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jan.util.db.DBRow;
import jan.util.db.SqlDBHelper;


public class MrLoc
{
	public class LTECell
	{
		public int Tac;
		public int EnbId;
		public long CellId;
		public double Longitude;
		public double Latitude;
		public int InDoor;
		public double Angle;
		
		public void Clear()
		{
			
		}		
	}	
	
   private static MrLoc instance = null;
   public static MrLoc GetInstance()
   {
	   if(instance == null)
	   {
		   instance = new MrLoc();
	   }
	   return instance;
   }
   
   private MrLoc()
   {
	   GridEgnoreAOA.put(180, 540);
   }
   
   public boolean init(String dbConnStr)
   {
	   String sql = "exec [sp_xdr_getltecell]";
	   List<DBRow> rows = SqlDBHelper.ExcuteQuery(dbConnStr, sql);
	   
	   lteCellMap.clear();
	   for(Object val : rows)
	   {
		   DBRow row = (DBRow)val;
		   
		   LTECell item = new LTECell();
		   
		   item.Tac = (int)row.get("itac");
		   item.EnbId = (int)row.get("ienodebid");
		   item.CellId = (long)row.get("ici");
		   item.Longitude = Double.parseDouble(row.get("flongitude").toString());
		   item.Latitude = Double.parseDouble(row.get("flatitude").toString());
		   item.InDoor = (int)row.get("iindoor");
		   item.Angle = Float.parseFloat(row.get("fangle").toString());
		   	   
		   addCell(item);
	   }
	   
	   return true;
   }
   
   
   private Map<Long, LTECell> lteCellMap = new HashMap<Long, LTECell>();  
   public void addCell(LTECell rec)
   {
   	  long nKey = rec.EnbId * 256 + rec.CellId; 
   	  lteCellMap.put(nKey, rec);
   }
   
   
   private float calcDist(StructData.SIGNAL_MR_All rec)
   {
	    try
		{
		   	if (rec.tsc.LteScTadv >= 0 && rec.tsc.LteScTadv < 1282)
		   	{
		   		return (float)((rec.tsc.LteScTadv + 0.5) * 16 * 4.89);
		   	}
		   	else if ((rec.tsc.LteScRTTD <= 2046) && (rec.tsc.LteScRTTD != -1000000))
		   	{
		   		return (float)((rec.tsc.LteScRTTD + 0.5) * 2 * 4.89);
		   	}
		   	else if (rec.tsc.LteScRTTD == 2047)
		   	{
		   		return (float)((rec.tsc.LteScRTTD + 2048) * 4.89);
		   	}
		   	else
		   	{
		   		if (rec.tsc.LteScRTTD != -1000000)
		   		{
		   			return (float)((2048 * 2 + (rec.tsc.LteScRTTD - 2048 + rec.tsc.LteScRTTD - 2048 + 1) * 4) * 4.89);
		   		}
		   		else
		   		{
		   			return 30000;
		   		}
		   	}			
		}
		catch (Exception e)
		{
			return 30000;
		}

   }
   
   
   private boolean calcLongLac(StructData.SIGNAL_MR_All rec, LTECell lcell, Integer longitude, Integer latitude)
   {
	   	final int GridEgnoreDist = 20000;
	   	final int MtLongitude = 100;
	   	final int MtLatitude = 90;
	   	final float PI = 3.14159f;
	
	   	longitude = 0;
	   	latitude = 0;
	   	double x = 0, y = 0;
	   	if (lcell.InDoor == 0)
	   	{
	   		float dist = calcDist(rec);
	   		if (dist > GridEgnoreDist) return false;
	   		double r = (double)((rec.tsc.LteScAOA / 2 + 0.25 + lcell.Angle) * PI / 180);
	
	   		x = dist * Math.sin(r);
	   		y = dist * Math.cos(r);
	   	}
	
	   	longitude = (int)(10000000 * lcell.Longitude + x * MtLongitude);
	   	latitude = (int)(10000000 * lcell.Latitude + y * MtLatitude);
	
	   	return true;
   }
   
   private Map<Integer, Integer> GridEgnoreAOA = new HashMap<Integer, Integer>();
   private void getLongLac(StructData.SIGNAL_MR_All rec, LTECell lcell, Integer longitude, Integer latitude)
   {
	   	longitude = 0;
	   	latitude = 0;
	
	   	if (rec.tsc.LteScAOA < 0) return;
	   	
	   	
	   	for(Map.Entry<Integer, Integer> item : GridEgnoreAOA.entrySet())
	   	{
	   		if(rec.tsc.LteScAOA >= item.getKey() && rec.tsc.LteScAOA >= item.getValue())
	   		{
	   			return;
	   		}
	   	}
	   	
	   	if (rec.tsc.LteScRTTD < 0 && (rec.tsc.LteScTadv < 0 || rec.tsc.LteScTadv >= 1282)) return;
	   	
	   	if (!calcLongLac(rec, lcell, longitude, latitude))
	   	{
	   		longitude = 0;
	   		latitude = 0;
	   	}
   }
   
   
   public void locLte(StructData.SIGNAL_MR_All rec, Integer longitude, Integer latitude)
   {
	   	if (lteCellMap.size() == 0)
	   	{
	   		return;
	   	}
	
	   	if ((rec.tsc.ENBId < 0) || (rec.tsc.CellId < 0))
	   	{
	   		return;
	   	}
	
	   	long nKey = rec.tsc.CellId;
	   	
	   	if(!lteCellMap.containsValue(nKey))
	   	{
	   		return;
	   	}
	   	
	   	rec.tsc.TAC = lteCellMap.get(nKey).Tac;
	
	   	getLongLac(rec, lteCellMap.get(nKey), longitude, latitude);
   }
   
   
}
