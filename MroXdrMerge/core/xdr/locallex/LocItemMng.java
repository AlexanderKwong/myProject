package xdr.locallex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.util.LOGHelper;
import util.Func;
import xdr.locallex.model.XdrDataBase;
import jan.util.GisFunction;
import jan.util.IWriteLogCallBack.LogType;

public class LocItemMng
{
	private long imsi;
	private int TimeSpan = 300;
	
	//zhaikaishun
	private long s1apid;
	private long eci;
	
	
	
	private HashMap<Integer, ArrayList<LocItem>> locLibMap = new HashMap<Integer, ArrayList<LocItem>>();// 将该用户的位置库按5分钟分开

	public long getImsi()
	{
		return imsi;
	}

	public void setImsi(long imsi)
	{
		this.imsi = imsi;
	}
	

	public long getS1apid()
	{
		return s1apid;
	}

	public void setS1apid(long s1apid)
	{
		this.s1apid = s1apid;
	}

	public long getEci()
	{
		return eci;
	}

	public void setEci(long eci)
	{
		this.eci = eci;
	}

	/**
	 * 将loclib 按照5分钟分包存放
	 * 
	 * @param loc
	 */
	public void addLocItem(LocItem loc)
	{
		int time = loc.itime;
		if (time > 0)
		{
			int key = time / TimeSpan * TimeSpan;
			ArrayList<LocItem> itemList = locLibMap.get(key);
			if (itemList == null)
			{
				itemList = new ArrayList<LocItem>();
				locLibMap.put(key, itemList);
			}
			itemList.add(loc);
		}
	}

	public ArrayList<XdrDataBase> fillLoc(ArrayList<XdrDataBase> itemList)
	{
		ArrayList<XdrDataBase> resItemList = new ArrayList<XdrDataBase>();

		XdrPacketMng xdrPacketMng = new XdrPacketMng();
		for (XdrDataBase item : itemList)
		{
			xdrPacketMng.addItem(item);
		}

		for (Map.Entry<Integer, ArrayList<XdrDataBase>> itemMap : xdrPacketMng.xdrDataMap.entrySet())
		{
			List<XdrDataBase> xdrDataList = fillLoc(itemMap.getKey(), itemMap.getValue());
			resItemList.addAll(xdrDataList);
		}
		return resItemList;

	}

	/**
	 * 同一个5分钟的http数据关联loc
	 * 
	 * @param itemList
	 * @param time
	 * @return
	 */
	public ArrayList<XdrDataBase> fillLoc(int time, ArrayList<XdrDataBase> itemList)
	{
		ArrayList<LocItem> nearLocList = getNearLocList(time);
		// LOGHelper.GetLogger().writeLog(LogType.debug, "find the
		// nearLocList,count is :" + nearLocList.size());

		LocItem locLibItem;
		ArrayList<XdrDataBase> locXdrDataList = new ArrayList<XdrDataBase>();
		for (XdrDataBase item : itemList)
		{
			locLibItem = getNearestLoc(item.getIstime(), nearLocList);
			if (locLibItem != null)
			{
				if (locLibItem.doorType == StaticConfig.ACTTYPE_OUT || locLibItem.doorType == 4)
				{
					if (Math.abs(locLibItem.itime - item.getIstime()) <= 20)
					{
//						LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(locLibItem.eci);
//						if(lteCellInfo!=null){
//							double dist = (long) GisFunction.GetDistance(locLibItem.ilongitude, locLibItem.ilatitude,
//									lteCellInfo.ilongitude, lteCellInfo.ilatitude);
							int maxRadius = 6000;
							//TODO zhaikaishun 2017-09-25 [下一次部署之前]
							/**
							 * 这个radius和maxRadius设置成多大，需要询问领导
							 */
//							maxRadius = Math.min(maxRadius, 5 * lteCellInfo.radius);  
//							if(dist<maxRadius){
								item.iCityID = locLibItem.cityID;
								item.iLongitude = locLibItem.ilongitude;
								item.iLatitude = locLibItem.ilatitude;
								item.iDoorType = StaticConfig.ACTTYPE_OUT;
								item.iAreaType = locLibItem.iAreaType;
								item.iAreaID = locLibItem.iAreaID;
								item.strloctp = locLibItem.loctp;
								item.iRadius = locLibItem.radius;
								item.ibuildid = locLibItem.ibuildid;
								item.iheight = locLibItem.iheight;
								item.testType = locLibItem.testType;
								item.label = locLibItem.label;
								item.locSource = Func.getLocSource(locLibItem.loctp);
								item.LteScRSRP = locLibItem.LteScRSRP;
								item.LteScSinrUL = locLibItem.LteScSinrUL;
								item.confidentType = locLibItem.confidentType;
								locXdrDataList.add(item);
//							}	
//						}
												
					}
				}
				else if (locLibItem.doorType == StaticConfig.ACTTYPE_IN || locLibItem.doorType == 3)
				{
					item.iCityID = locLibItem.cityID;
					item.iLongitude = locLibItem.ilongitude;
					item.iLatitude = locLibItem.ilatitude;
					item.iDoorType = StaticConfig.ACTTYPE_IN;
					item.iAreaType = locLibItem.iAreaType;
					item.iAreaID = locLibItem.iAreaID;
					item.strloctp = locLibItem.loctp;
					item.iRadius = locLibItem.radius;
					item.ibuildid = locLibItem.ibuildid;
					item.iheight = locLibItem.iheight;
					item.testType = locLibItem.testType;
					item.label = locLibItem.label;
					
					item.locSource = Func.getLocSource(locLibItem.loctp);
					item.LteScRSRP = locLibItem.LteScRSRP;
					item.LteScSinrUL = locLibItem.LteScSinrUL;
					
					item.confidentType = locLibItem.confidentType;
					locXdrDataList.add(item);
				}

			}
		}
		return locXdrDataList;
	}

	public ArrayList<LocItem> getNearLocList(int itime)
	{
		int Ctime = itime / 300 * 300;
		ArrayList<LocItem> LocList0 = locLibMap.get(Ctime - TimeSpan);
		ArrayList<LocItem> LocList1 = locLibMap.get(Ctime);
		ArrayList<LocItem> LocList2 = locLibMap.get(Ctime + TimeSpan);
		ArrayList<LocItem> BF5MinLocList = new ArrayList<LocItem>();
		if (LocList0 != null)
		{
			BF5MinLocList.addAll(LocList0);
		}
		if (LocList1 != null)
		{
			BF5MinLocList.addAll(LocList1);
		}
		if (LocList2 != null)
		{
			BF5MinLocList.addAll(LocList2);
		}
		return BF5MinLocList;
	}

	public LocItem getNearestLoc(int tmTime, ArrayList<LocItem> LocList)
	{
		int curOutTime = TimeSpan * 2;
		int curInTime = TimeSpan * 2;
		int curRsrpTime = TimeSpan * 2;
		LocItem curOutItem = null;
		LocItem curInItem = null;
		LocItem curRsrpItem = null;

		int spTime;

		for (LocItem item : LocList)
		{
			if ((item.doorType == StaticConfig.ACTTYPE_OUT || item.doorType ==4 ) && item.ilongitude > 0)
			{
				spTime = Math.abs(tmTime - item.itime);
				if (spTime < curOutTime)
				{
					curOutTime = spTime;
					curOutItem = item;
				}
			}
			else if ((item.doorType == StaticConfig.ACTTYPE_IN || item.doorType ==3) && item.ilongitude > 0)
			{
				spTime = Math.abs(tmTime - item.itime);
				if (spTime < curInTime)
				{
					curInTime = spTime;
					curInItem = item;
				}
			}
			
			if (item.LteScRSRP > -1000)
			{
				spTime = Math.abs(tmTime - item.itime);
				if (spTime < curRsrpTime)
				{
					curRsrpTime = spTime;
					curRsrpItem = item;
				}
			}
		}

		if (curOutItem != null)
		{
			if (curOutItem.LteScRSRP < -1000 && curRsrpItem != null)
			{
				curOutItem.LteScRSRP = curRsrpItem.LteScRSRP;
			}
			return curOutItem;
		}
		else if (curInItem != null)
		{
			if (curInItem.LteScRSRP < -1000 && curRsrpItem != null)
			{
				curInItem.LteScRSRP = curRsrpItem.LteScRSRP;
			}

			return curInItem;
		}
		return null;
	}

	private class XdrPacketMng
	{
		public HashMap<Integer, ArrayList<XdrDataBase>> xdrDataMap = new HashMap<Integer, ArrayList<XdrDataBase>>();

		public void addItem(XdrDataBase xdrData)
		{
			int time = xdrData.getIstime() / TimeSpan * TimeSpan;
			ArrayList<XdrDataBase> xdrDataList = xdrDataMap.get(time);
			if (xdrDataList == null)
			{
				xdrDataList = new ArrayList<XdrDataBase>();
				xdrDataMap.put(time, xdrDataList);
			}
			xdrDataList.add(xdrData);
		}
	}

}
