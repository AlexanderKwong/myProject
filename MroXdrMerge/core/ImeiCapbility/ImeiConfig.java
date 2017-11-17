package ImeiCapbility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class ImeiConfig
{
	private static ImeiConfig instance;
	private HashMap<Integer, Integer> ImeiCapbilityMap;

	public static ImeiConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new ImeiConfig();
		}
		return instance;
	}

	private ImeiConfig()
	{
		ImeiCapbilityMap = new HashMap<Integer, Integer>();
	}

	public boolean loadImeiCapbility(Configuration conf)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getImeiConfigPath();
		try
		{
			BufferedReader reader = null;
			try
			{
				if (!filePath.contains(":"))
				{
					HDFSOper hdfsOper = new HDFSOper(conf);
					if (!hdfsOper.checkFileExist(filePath))
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "imeiconfig  is not exists: " + filePath);
						return false;
					}
					reader = new BufferedReader(
							new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
				}
				else if (new File(filePath).exists())
				{
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
				}
				else
				{
					return false;
				}
				String strData = "";
				while ((strData = reader.readLine()) != null)
				{
					if (strData.trim().length() == 0)
					{
						continue;
					}
					try
					{
						String[] val = strData.split("\t");
						if (val.length == 2)
						{
							ImeiCapbilityMap.put(Integer.parseInt(val[0]), Integer.parseInt(val[1]));
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "imeiconfig  error : " + strData, e);
						return false;
					}
				}
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
			LOGHelper.GetLogger().writeLog(LogType.error, "imeiconfig  error ", e);
			return false;
		}
		return true;
	}

	public int getValue(int imei)
	{
		if (ImeiCapbilityMap.get(imei) == null)
		{
			return 0;
		}
		else
		{
			return ImeiCapbilityMap.get(imei);
		}
	}

}
