package xdr.lablefill;

import StructData.NC_LTE;
import cellconfig.LteCellConfig;
import cellconfig.LteCellInfo;

public class MrOutGridCellNc implements IResultTable
{
	public int iCityID = 0;
	public int iLongitude = 0;
	public int iLatitude = 0;
	public int iECI = 0;
	public int iEarfcn = 0;
	public int iPci = 0;
	public int iTime = 0;
	public int iASNei_MRCnt = 0;
	public double fASNei_RSRPValue = 0;
	
	public MrOutGridCellNc(MrOutGridCell item, NC_LTE nclte)
	{
		iCityID = item.iCityID;
		iLongitude = item.iLongitude;
		iLatitude = item.iLatitude;
		iECI = item.iECI;
		iEarfcn = nclte.LteNcEarfcn;
		iPci = nclte.LteNcPci;
		iTime = item.iTime;
	}
	
	public static final String TypeName = "mroutgridcellnc";
	
	public static String getKey(MrOutGridCell item, NC_LTE nclte)
	{
		//此处按天计算,所以没有添加时间因素
		return item.iCityID 
				+ "_" + item.iLongitude
				+ "_" + item.iLatitude
				+ "_" + item.iECI
				+ "_" + nclte.LteNcEarfcn
				+ "_" + nclte.LteNcPci;
	}
	
	public void Stat(NC_LTE nclte)
	{
		if (nclte.LteNcRSRP != -1000000)
		{
			iASNei_MRCnt++;
			fASNei_RSRPValue += nclte.LteNcRSRP;
		}
	}
	
    
	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		String tabMark = ResultHelper.TabMark;
		
		sb.append(iCityID);
		sb.append(tabMark);sb.append(iLongitude);
		sb.append(tabMark);sb.append(iLatitude);
		sb.append(tabMark);sb.append(iECI);
		sb.append(tabMark);sb.append(getNbEci());
		sb.append(tabMark);sb.append(iEarfcn);
		sb.append(tabMark);sb.append(iPci);
		sb.append(tabMark);sb.append(iTime);
		sb.append(tabMark);sb.append(iASNei_MRCnt);
		sb.append(tabMark);sb.append(fASNei_RSRPValue);

		return sb.toString();
	}
	
	public long getNbEci()
	{
		LteCellInfo nbCell = null;
		try
		{
			nbCell = LteCellConfig.GetInstance().getNearestCell(iECI, iEarfcn, iPci);
			if(nbCell != null)return nbCell.eci;	
		}
		catch (Exception e)
		{
		}
		return -1;
	}

}
