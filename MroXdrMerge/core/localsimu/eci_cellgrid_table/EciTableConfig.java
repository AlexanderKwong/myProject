package localsimu.eci_cellgrid_table;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;

public class EciTableConfig
{
	private HashMap<Long, Integer> eciTableMap;
	private static EciTableConfig instance;

	public static EciTableConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new EciTableConfig();
		}
		return instance;
	}

	private EciTableConfig()
	{
		eciTableMap = new HashMap<Long, Integer>();
	}

	public HashMap<Long, Integer> getEciConfigTableMap()
	{
		return eciTableMap;
	}

	public boolean loadEnbidTable(Configuration conf, String filePath)
	{
		try
		{
			HDFSOper hdfsOper = new HDFSOper(conf);
			if (!hdfsOper.checkFileExist(filePath))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "EciTableConfig config is not exists: " + filePath);
				return false;
			}

			Path path = new Path(filePath);
			BufferedReader reader = null;
			eciTableMap = new HashMap<Long, Integer>();
			try
			{
				reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(path), "UTF-8"));
				String strData;
				long eci;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.trim().length() == 0)
					{
						continue;
					}
					eci = Long.parseLong(strData.trim());
					eciTableMap.put(eci, 1);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "EnbidTableConfig error ", e);
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
			LOGHelper.GetLogger().writeLog(LogType.error, "loadLteCell error ", e);
			return false;
		}
		return true;
	}
}
