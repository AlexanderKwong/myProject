package cellconfig;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class CellBuildInfo
{

	private String cellBuildPath = "";
	private HashMap<String, Integer> cellBuildMap;

	public CellBuildInfo() throws IOException
	{
		cellBuildPath = MainModel.GetInstance().getAppConfig().getCellBuildPath();
		cellBuildMap = new HashMap<String, Integer>();
	}

	public boolean loadCellBuild(Configuration conf, long eci, int cityid)
	{
		String path = cellBuildPath + "/cell_build_grid_" + cityid + "_" + eci + ".data.gz";
		return loadCellBuild(conf, path);
	}

	public static void main(String args[])
	{
		try
		{
			CellBuildInfo tempcellbuild = new CellBuildInfo();
			tempcellbuild.loadCellBuild(null, "D:\\cell_build_grid_79691581.data.gz");
			for (String key : tempcellbuild.cellBuildMap.keySet())
			{
				System.out.print(key + ":");
				System.out.println(tempcellbuild.cellBuildMap.get(key));
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean loadCellBuild(Configuration conf, String filePath)
	{
		DataInputStream reader = null;
		try
		{
			if (!filePath.contains(":"))
			{
				HDFSOper hdfsOper = new HDFSOper(conf);
				if (!hdfsOper.checkFileExist(filePath))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellBuild config is not exists: " + filePath);
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
			int eci = reader.readInt();
			int count = reader.readInt();
			for (int i = 0; i < count; i++)
			{
				int buildid = reader.readInt();
				int length = reader.readInt();
				for (int j = 0; j < length; j++)
				{
					int longtitude = reader.readInt();
					int latitude = reader.readInt();
					String key = longtitude + "_" + latitude;
					cellBuildMap.put(key, buildid);
				}
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "loadcellBuild error " + filePath, e);
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

	public HashMap<String, Integer> getCellBuildMap()
	{
		return cellBuildMap;
	}

	public int getBuildId(int longtitude, int latitude)
	{
		String gridString = (longtitude / 1000 * 1000) + "_" + ((latitude / 900) * 900 + 900);
		if (cellBuildMap.get(gridString) != null)
		{
			return cellBuildMap.get(gridString);
		}
		return 0;
	}
}
