package StructData;

import java.util.HashMap;
import java.util.Map;

import jan.util.data.MyInt;

public class Stat_CellGrid_2G
{
	public int icityid;
	public int iLac;
	public long iCi;
	public int startTime;
	public int endTime;
	public int iduration;
	public int idistance;
	public int isamplenum;

	public int itllongitude;
	public int itllatitude;
	public int ibrlongitude;
	public int ibrlatitude;
	
	public int UserCount;
	public int XdrCount;
	
	
	public Map<Long, MyInt> imsiMap;
	
	public Stat_CellGrid_2G()
	{
		imsiMap = new HashMap<Long, MyInt>();
		Clear();
	}
	
	public void Clear()
	{
		XdrCount = 0;
		
		imsiMap.clear();
	};
	
	public static Stat_CellGrid_2G FillData(String[] values, int startPos)
	{
		int i = startPos;
		
		Stat_CellGrid_2G item = new Stat_CellGrid_2G();
		item.icityid = Integer.parseInt(values[i++]);          
		item.iLac = Integer.parseInt(values[i++]); 
		item.iCi = Long.parseLong(values[i++]); 
		item.startTime = Integer.parseInt(values[i++]); 
		item.endTime = Integer.parseInt(values[i++]);   
		item.iduration = Integer.parseInt(values[i++]); 
		item.idistance = Integer.parseInt(values[i++]); 
		item.isamplenum = Integer.parseInt(values[i++]);
		item.itllongitude = Integer.parseInt(values[i++]);
		item.itllatitude = Integer.parseInt(values[i++]);
		item.ibrlongitude = Integer.parseInt(values[i++]);
		item.ibrlatitude = Integer.parseInt(values[i++]); 
			
        item.UserCount = Integer.parseInt(values[i++]); 
        item.XdrCount = Integer.parseInt(values[i++]); 
				
		return item;
	}
	
}
