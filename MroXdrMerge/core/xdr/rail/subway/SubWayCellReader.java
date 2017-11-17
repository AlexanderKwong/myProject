package xdr.rail.subway;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class SubWayCellReader
{
	public static HashMap<Integer,Integer> cellListMap;
	private static SubWayCellReader instance = null;
	
	public static void main(String[] args)
	{
		HashMap<Integer,Integer> eciMap = SubWayCellReader.GetInstance().getEciMap();
		System.out.println(eciMap.size()); 
	}
	
	private SubWayCellReader(){
		cellListMap = new HashMap<Integer,Integer>();
	}

	public static SubWayCellReader GetInstance(){
		if (instance == null)
		{
			instance = new SubWayCellReader();
		}
		return instance;
	}
	
	public HashMap<Integer,Integer> getEciMap(){
		
		if(cellListMap.size()==0){
			instance.readCellList();
		}
		return cellListMap;
	}
	
	public void readCellList()
	{
		String inputPath = "subway_cell_list.txt";
		InputStream is;
		try
		{
			is = getClass().getClassLoader().getResourceAsStream(inputPath);
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = "";
			while ((line = br.readLine()) != null)
			{
				String[] split = line.split("-");
				if(split.length == 4){
					int eci = Integer.parseInt(split[2])*256+Integer.parseInt(split[3]);
					cellListMap.put(eci, 1);
				}
			}
			br.close();
			is.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

}
