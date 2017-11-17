package cellconfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import StructData.StaticConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.GisFunction;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class CellConfig
{
	private static CellConfig instance;

	public static CellConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new CellConfig();
		}
		return instance;
	}

	public static void main(String[] args) throws Exception
	{
		// Configuration conf = new Configuration();
		// if (!CellConfig.GetInstance().loadLteCell(conf))
		// {
		// LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error
		// 请检查！");
		// throw (new IOException("ltecell init error 请检查！"));
		// }
		CellConfig.GetInstance().loadLteCell(null, "D:\\tb_cfg_city_cell_td.txt");
		System.out.println(CellConfig.GetInstance().lteCellInfoMap.size());
	}

	private CellConfig()
	{
		lteCellInfoMap = new HashMap<Long, LteCellInfo>();
		fcnPciLteCellMap = new HashMap<Long, List<LteCellInfo>>();

		gsmCellInfoMap = new HashMap<Long, GsmCellInfo>();
		bcchBsicGsmCellMap = new HashMap<Long, List<GsmCellInfo>>();

		tdCellInfoMap = new HashMap<Long, TdCellInfo>();
		fcnPciTdCellMap = new HashMap<Long, List<TdCellInfo>>();
	}

	////////////////////////////////////////////////// tdlte
	////////////////////////////////////////////////// ///////////////////////////////////////////////
	public Map<Long, LteCellInfo> lteCellInfoMap;
	public Map<Long, List<LteCellInfo>> fcnPciLteCellMap;

	public String errLog = "";

	public Map<Long, LteCellInfo> getLteCellInfoMap()
	{
		return lteCellInfoMap;
	}

	public boolean loadLteCell(Configuration conf)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getLteCellConfigPath();
		return loadLteCell(conf, filePath);
	}

	// 数据来源：select distinct enbid,小区标识,地市id,Round(isnull(b.radius, 5000),2) as
	// 理想覆盖半径,经度,纬度,isnull(convert(int,载频频点), 0) as 载频频点,isnull(pci, 0) as pci
	// from dbo.tb_v2_lte小区_全部 a left join [tb_cfg_designed_radius] b on enbid *
	// 256 + 小区标识 = b.[eci]
	// 导出后，需要转化为unicode，去掉包头
	// "/mt_wlyh/Data/config/tb_cfg_city_cell.txt";
	public boolean loadLteCell(Configuration conf, String filePath)
	{
		try
		{
			int noRadis = 0;
			BufferedReader reader = null;
			lteCellInfoMap = new HashMap<Long, LteCellInfo>();
			fcnPciLteCellMap = new HashMap<Long, List<LteCellInfo>>();
			try
			{
				if (!filePath.contains(":"))
				{
					HDFSOper hdfsOper = new HDFSOper(conf);
					if (!hdfsOper.checkFileExist(filePath))
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "config is not exists: " + filePath);
						return false;
					}

					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				}
				else
				{
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
				}
				String strData;
				String[] values;
				long eci;
				long fcnPciKey;
				List<LteCellInfo> fcnPciList = null;
				while ((strData = reader.readLine()) != null)
				{
					// System.out.println(strData);

					if (strData.trim().length() == 0)
					{
						continue;
					}

					try
					{
						values = strData.split("\t", -1);// |\t
						if (values.length < 12)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "cell config error: " + strData);
							continue;
						}
						LteCellInfo item = LteCellInfo.FillData(values);
						if (item.radius <= 0)
						{
							noRadis++;
						}
						if (noRadis >= 5000)
						{
							return false;
						}
						if (item.cityid == StaticConfig.Int_Abnormal)
						{
							item.cityid = -1;
						}
						if (item.enbid > 0 && item.cellid > 0)
						{
							eci = item.enbid * 256 + item.cellid;
							lteCellInfoMap.put(eci, item);
						}

						if (item.pci > 0 && item.fcn > 0 && item.cityid > 0)
						{
							fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", item.cityid, item.fcn, item.pci));
							fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
							if (fcnPciList == null)
							{
								fcnPciList = new ArrayList<LteCellInfo>();
								fcnPciLteCellMap.put(fcnPciKey, fcnPciList);
							}
							fcnPciList.add(item);
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
						LOGHelper.GetLogger().writeLog(LogType.error, "loadLteCell error : " + strData, e);
						errLog = "loadLteCell error : " + e.getMessage() + ":" + strData;
						return false;
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				LOGHelper.GetLogger().writeLog(LogType.error, "loadLteCell error ", e);
				errLog = "loadLteCell error : " + e.getMessage();
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
			LOGHelper.GetLogger().writeLog(LogType.error, "loadLteCell error ", e);
			errLog = "loadLteCell error : " + e.getMessage();
			return false;
		}

		return true;
	}

	public int getCellCityID(long eci)
	{
		LteCellInfo cellInfo = lteCellInfoMap.get(eci);
		if (cellInfo == null)
		{
			return -1;
		}
		return cellInfo.cityid;
	}

	public LteCellInfo getLteCell(long eci)
	{
		return lteCellInfoMap.get(eci);
	}

	public int getlteCellInfoMapSize()
	{
		return lteCellInfoMap.size();
	}

	public LteCellInfo getNearestCell(int longtitude, int latitude, int cityID, int fcn, int pci)
	{
		if (longtitude <= 0 || latitude <= 0 || cityID <= 0 || fcn <= 0 || pci <= 0)
		{
			return null;
		}

		long fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", cityID, fcn, pci));
		List<LteCellInfo> fcnPciList = null;
		fcnPciList = fcnPciLteCellMap.get(fcnPciKey);
		if (fcnPciList == null)
		{
			return null;
		}
		int distance = Integer.MAX_VALUE;
		int curDistance = 0;
		LteCellInfo nearestCell = null;
		for (LteCellInfo item : fcnPciList)
		{
			if (Math.abs(longtitude - item.ilongitude) > 600000 || Math.abs(latitude - item.ilatitude) > 600000)
				continue;
			curDistance = (int) GisFunction.GetDistance(longtitude, latitude, item.ilongitude, item.ilatitude);
			if (curDistance < distance)
			{
				nearestCell = item;
				distance = curDistance;
			}
		}
		// if(distance >= 6000)
		// {
		// return null;
		// }
		return nearestCell;
	}

	////////////////////////////////////////////////// tdlte
	////////////////////////////////////////////////// ///////////////////////////////////////////////

	////////////////////////////////////////////////// gsm
	////////////////////////////////////////////////// ///////////////////////////////////////////////
	private Map<Long, GsmCellInfo> gsmCellInfoMap;
	private Map<Long, List<GsmCellInfo>> bcchBsicGsmCellMap;

	public Map<Long, GsmCellInfo> getGsmCellInfoMap()
	{
		return gsmCellInfoMap;
	}

	// 数据来源：select distinct a.lac,a.ci,基站id, 地市id,Round(isnull(b.radius,
	// 5000),2),经度,纬度,BCCH,bsic
	// from dbo.tb_v2_gsm小区_全部 a left join tb_cfg_designed_radius_gsm b
	// on a.LAC = b.lac and a.CI = b.ci
	// where BCCH > 0
	// 导出后，需要转化为unicode，去掉包头
	public boolean loadGsmCell(Configuration conf)
	{
		try
		{
			HDFSOper hdfsOper = new HDFSOper(conf);
			String filePath = MainModel.GetInstance().getAppConfig().getGsmCellConfigPath();// "/mt_wlyh/Data/config/tb_cfg_city_cell_gsm.txt";
			if (!hdfsOper.checkFileExist(filePath))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "config not exists : " + filePath);
				return false;
			}

			BufferedReader reader = null;
			gsmCellInfoMap = new HashMap<Long, GsmCellInfo>();
			bcchBsicGsmCellMap = new HashMap<Long, List<GsmCellInfo>>();
			try
			{
				reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				String strData;
				String[] values;
				long bcchBsicKey;
				List<GsmCellInfo> bcchBsicList = null;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.length() == 0)
					{
						continue;
					}

					try
					{
						values = strData.split("\t", -1);
						GsmCellInfo item = GsmCellInfo.FillData(values);
						if (item.lac > 0 && item.ci > 0)
						{
							gsmCellInfoMap.put(makeGsmCellKey(item.lac, item.ci), item);
						}

						if (item.bcch > 0 && item.bsic > 0 && item.cityid > 0)
						{
							bcchBsicKey = Long.parseLong(String.format("%02d%05d%02d", item.cityid, item.bcch, item.bsic));
							bcchBsicList = bcchBsicGsmCellMap.get(bcchBsicKey);
							if (bcchBsicList == null)
							{
								bcchBsicList = new ArrayList<GsmCellInfo>();
								bcchBsicGsmCellMap.put(bcchBsicKey, bcchBsicList);
							}
							bcchBsicList.add(item);
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "loadGsmCell error : " + strData, e);
						return false;
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "loadGsmCell error ", e);
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "loadGsmCell error ", e);
			return false;
		}

		return true;
	}

	public int getGsmCellCityID(int lac, int ci)
	{
		GsmCellInfo cellInfo = getGsmCell(lac, ci);
		if (cellInfo == null)
		{
			return -1;
		}
		return cellInfo.cityid;
	}

	public GsmCellInfo getGsmCell(int lac, long ci)
	{
		return gsmCellInfoMap.get(makeGsmCellKey(lac, ci));
	}

	public GsmCellInfo getNearestGsmCell(int longtitude, int latitude, int cityID, int bcch, int bsic)
	{
		if (longtitude <= 0 || latitude <= 0 || cityID <= 0 || bcch <= 0 || bsic <= 0)
		{
			return null;
		}

		long bcchBsicKey = Long.parseLong(String.format("%02d%05d%02d", cityID, bcch, bsic));
		List<GsmCellInfo> bcchBsicList = null;
		bcchBsicList = bcchBsicGsmCellMap.get(bcchBsicKey);
		if (bcchBsicList == null)
		{
			return null;
		}
		int distance = Integer.MAX_VALUE;
		int curDistance = 0;
		GsmCellInfo nearestCell = null;
		for (GsmCellInfo item : bcchBsicList)
		{
			curDistance = (int) GisFunction.GetDistance(longtitude, latitude, item.ilongitude, item.ilatitude);
			if (curDistance < distance)
			{
				nearestCell = item;
				distance = curDistance;
			}
		}
		// if(distance >= 6000)
		// {
		// return null;
		// }
		return nearestCell;
	}

	public static long makeGsmCellKey(int lac, long ci)
	{
		return (long) lac * 100000 + ci;
	}

	////////////////////////////////////////////////// gsm
	////////////////////////////////////////////////// ///////////////////////////////////////////////

	////////////////////////////////////////////////// tdscdma
	////////////////////////////////////////////////// ///////////////////////////////////////////////
	private Map<Long, TdCellInfo> tdCellInfoMap;
	private Map<Long, List<TdCellInfo>> fcnPciTdCellMap;

	public Map<Long, TdCellInfo> getTDCellInfoMap()
	{
		return tdCellInfoMap;
	}

	// 数据来源：select distinct a.lac,a.ci,基站id, 地市id,Round(isnull(b.radius,
	// 5000),2),经度,纬度,主载频频点,扰码
	// from dbo.tb_v2_td小区_全部 a left join tb_cfg_designed_radius_tds b
	// on a.LAC = b.lac and a.CI = b.ci
	// where 主载频频点 > 0
	// 导出后，需要转化为unicode，去掉包头
	public boolean loadTdCell(Configuration conf)
	{
		try
		{
			HDFSOper hdfsOper = new HDFSOper(conf);
			String filePath = MainModel.GetInstance().getAppConfig().getTDCellConfigPath();// "/mt_wlyh/Data/config/tb_cfg_city_cell_td.txt";
			if (!hdfsOper.checkFileExist(filePath))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "config not exists : " + filePath);
				return false;
			}

			BufferedReader reader = null;
			tdCellInfoMap = new HashMap<Long, TdCellInfo>();
			fcnPciTdCellMap = new HashMap<Long, List<TdCellInfo>>();
			try
			{
				reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				String strData;
				String[] values;
				long fcnPciKey;
				List<TdCellInfo> fcnPciList = null;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.length() == 0)
					{
						continue;
					}

					try
					{
						values = strData.split("\t", -1);
						TdCellInfo item = TdCellInfo.FillData(values);
						if (item.lac > 0 && item.ci > 0)
						{
							tdCellInfoMap.put(makeGsmCellKey(item.lac, item.ci), item);
						}

						if (item.fcn > 0 && item.pci > 0 && item.cityid > 0)
						{
							fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", item.cityid, item.fcn, item.pci));
							fcnPciList = fcnPciTdCellMap.get(fcnPciKey);
							if (fcnPciList == null)
							{
								fcnPciList = new ArrayList<TdCellInfo>();
								fcnPciTdCellMap.put(fcnPciKey, fcnPciList);
							}
							fcnPciList.add(item);
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "loadTdCell error : " + strData, e);
						return false;
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "loadTdCell error ", e);
				return false;
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "loadTdCell error ", e);
			return false;
		}

		return true;
	}

	public int getTdCellCityID(int lac, int ci)
	{
		TdCellInfo cellInfo = getTdCell(lac, ci);
		if (cellInfo == null)
		{
			return -1;
		}
		return cellInfo.cityid;
	}

	public TdCellInfo getTdCell(int lac, long ci)
	{
		return tdCellInfoMap.get(makeTdCellKey(lac, ci));
	}

	public TdCellInfo getNearestTdCell(int longtitude, int latitude, int cityID, int fcn, int pci)
	{
		if (longtitude <= 0 || latitude <= 0 || cityID <= 0 || fcn <= 0 || pci <= 0)
		{
			return null;
		}

		long fcnPciKey = Long.parseLong(String.format("%02d%05d%03d", cityID, fcn, pci));
		List<TdCellInfo> fcnPciList = null;
		fcnPciList = fcnPciTdCellMap.get(fcnPciKey);
		if (fcnPciList == null)
		{
			return null;
		}
		int distance = Integer.MAX_VALUE;
		int curDistance = 0;
		TdCellInfo nearestCell = null;
		for (TdCellInfo item : fcnPciList)
		{
			curDistance = (int) GisFunction.GetDistance(longtitude, latitude, item.ilongitude, item.ilatitude);
			if (curDistance < distance)
			{
				nearestCell = item;
				distance = curDistance;
			}
		}
		// if(distance >= 6000)
		// {
		// return null;
		// }
		return nearestCell;
	}

	public static long makeTdCellKey(int lac, long ci)
	{
		return (long) lac * 100000 + ci;
	}

	////////////////////////////////////////////////// tdscdma
	////////////////////////////////////////////////// ///////////////////////////////////////////////

}
