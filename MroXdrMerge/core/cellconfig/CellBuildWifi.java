package cellconfig;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mroxdrmerge.MainModel;

public class CellBuildWifi
{
	private String cellBuildWifiPath = "";
	private HashMap<String, Integer> cellBuildWifiMap;

	public CellBuildWifi() throws IOException
	{
		cellBuildWifiPath = MainModel.GetInstance().getAppConfig().getCellBuildWifiPath();
		cellBuildWifiMap = new HashMap<String, Integer>();
	}

	public static void main(String args[])
	{
		try
		{
			CellBuildWifi tempcellbuild = new CellBuildWifi();
			tempcellbuild.loadWifi(null, "D:\\cell_build_wifi_7345793.data.gz");
			for (String key : tempcellbuild.cellBuildWifiMap.keySet())
			{
				System.out.print(key + ":");
				System.out.println(tempcellbuild.cellBuildWifiMap.get(key));
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean loadBuildWifi(Configuration conf, int eci, int cityid)
	{
		String path = cellBuildWifiPath + "/cell_build_wifi_" + cityid + "_" + eci + ".data.gz";
		return loadWifi(conf, path);
	}

	public boolean loadWifi(Configuration conf, String filePath)
	{
		DataInputStream reader = null;
		try
		{
			if (!filePath.contains(":"))
			{
				HDFSOper hdfsOper = new HDFSOper(conf);
				if (!hdfsOper.checkFileExist(filePath))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellBuildWifi config is not exists: " + filePath);
					return false;
				}
				reader = new DataInputStream(new GZIPInputStream(hdfsOper.getHdfs().open(new Path(filePath))));
			}
			else if (filePath.endsWith(".gz"))
			{
				reader = new DataInputStream(new GZIPInputStream(new FileInputStream(filePath)));
			}
			else
			{
				reader = new DataInputStream(new FileInputStream(filePath));
			}
			byte[] ssid = new byte[6];
			int eci = reader.readInt();
			int count = reader.readInt();
			for (int i = 0; i < count; i++)
			{
				int buildid = reader.readInt();
				int length = reader.readInt();
				for (int j = 0; j < length; j++)
				{
					int level = reader.readShort();
					reader.readFully(ssid);
					String mac = bytesToHexString(ssid);
					String key = buildid + "_" + mac;
					cellBuildWifiMap.put(key, level);
				}
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "loadcellBuildWifi error " + filePath, e);
			return false;
		}
		finally
		{
			if (reader != null)
			{
				try
				{
					reader.close();
				}
				catch (IOException e)
				{

				}
			}
		}
		return true;
	}

	public static String bytesToHexString(byte[] src)
	{
		StringBuilder stringBuilder = new StringBuilder("");
		if (src == null || src.length <= 0)
		{
			return null;
		}
		for (int i = 0; i < src.length; i++)
		{
			int v = src[i] & 0xFF;
			String hv = Integer.toHexString(v);
			if (hv.length() < 2)
			{
				stringBuilder.append(0);
			}
			stringBuilder.append(hv);
		}
		return stringBuilder.toString();
	}

	public HashMap<String, Integer> getCellBuildWifiMap()
	{
		return cellBuildWifiMap;
	}

	public int getLevel(int buildid, String mac)
	{
		String key = buildid + "_" + mac;
		if (cellBuildWifiMap.get(key) != null)
		{
			return cellBuildWifiMap.get(key) * 3;
		}
		return -1;
	}
}
