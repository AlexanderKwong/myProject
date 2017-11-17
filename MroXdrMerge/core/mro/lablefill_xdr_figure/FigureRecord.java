package mro.lablefill_xdr_figure;

import java.util.HashMap;

import util.DataGeter;

public class FigureRecord
{
	private int buildingId;
	private long ilongitude;
	private long ilatitude;
	private int level;
	private String type; // 记录指纹是10*10的，还是40*40的、40*40low的
	private HashMap<Long, FigureCell> eci_GridCellMap = new HashMap<Long, FigureCell>();
	private HashMap<Integer, FigureCell> earfcn_pci_GridCellMap = new HashMap<Integer, FigureCell>();
	public static final String spliter = ",";
	public static final String spliter2 = "|";

	@SuppressWarnings("unused")
	public FigureRecord(String value)
	{
		String tmp[] = value.split("\\|", -1);
		for (int i = 0; i < tmp.length; i++)
		{
			String basicInfo[] = tmp[i].split(",");
			if (i == 0)
			{
				type = basicInfo[0].split("_")[0];
				buildingId = DataGeter.GetInt(basicInfo[0].split("_")[1], 0);
				ilongitude = DataGeter.GetLong(basicInfo[2], 0);
				ilatitude = DataGeter.GetLong(basicInfo[3], 0);
				level = DataGeter.GetInt(basicInfo[4], 0);
				FigureCell figureCell = new FigureCell(DataGeter.GetInt(basicInfo[1], 0),
						DataGeter.GetDouble(basicInfo[5], 0), DataGeter.GetInt(basicInfo[6], 0),
						DataGeter.GetInt(basicInfo[7], 0), DataGeter.GetLong(basicInfo[8], 0),
						DataGeter.GetLong(basicInfo[9], 0));
				eci_GridCellMap.put(figureCell.getEci(), figureCell);
				earfcn_pci_GridCellMap.put((figureCell.getEarfcn() * 1000 + figureCell.getPci()), figureCell);
			}
			else
			{
				FigureCell figureCell = new FigureCell(DataGeter.GetInt(basicInfo[0], 0),
						DataGeter.GetDouble(basicInfo[1], 0), DataGeter.GetInt(basicInfo[2], 0),
						DataGeter.GetInt(basicInfo[3], 0), DataGeter.GetLong(basicInfo[4], 0),
						DataGeter.GetLong(basicInfo[5], 0));
				eci_GridCellMap.put(figureCell.getEci(), figureCell);
				earfcn_pci_GridCellMap.put((figureCell.getEarfcn() * 1000 + figureCell.getPci()), figureCell);
			}
		}
	}

	public int getBuildingId()
	{
		return buildingId;
	}

	public long getIlongitude()
	{
		return ilongitude;
	}

	public long getIlatitude()
	{
		return ilatitude;
	}

	public int getLevel()
	{
		return level;
	}

	public HashMap<Long, FigureCell> getEci_GridCellMap()
	{
		return eci_GridCellMap;
	}

	public HashMap<Integer, FigureCell> getEarfcn_pci_GridCellMap()
	{
		return earfcn_pci_GridCellMap;
	}

	public String getType()
	{
		return type;
	}

	public GridKey returnGridKey()
	{
		return new GridKey(ilongitude, ilatitude, level);
	}

	public String fgGridToString()
	{
		StringBuffer sbf = new StringBuffer();
		sbf.append(type + "_");
		sbf.append(buildingId);
		sbf.append(spliter);
		if (eci_GridCellMap.size() > 0)
		{
			int i = 0;
			for (long eci : eci_GridCellMap.keySet())
			{
				if (i == 0)
				{
					sbf.append(eci_GridCellMap.get(eci).getEci());
					sbf.append(spliter);
					sbf.append(ilongitude);
					sbf.append(spliter);
					sbf.append(ilatitude);
					sbf.append(spliter);
					sbf.append(level);
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getRsrp());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getEarfcn());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getPci());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getGongcanIlongitude());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getGongcanIlatitud());
					sbf.append(spliter2);
					i++;
				}
				else
				{
					sbf.append(eci_GridCellMap.get(eci).getEci());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getRsrp());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getEarfcn());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getPci());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getGongcanIlongitude());
					sbf.append(spliter);
					sbf.append(eci_GridCellMap.get(eci).getGongcanIlatitud());
					sbf.append(spliter2);
				}
			}
		}
		if (sbf.length() > 0)
		{
			return sbf.substring(0, sbf.length() - 1);
		}
		return sbf.toString();

	}

}
