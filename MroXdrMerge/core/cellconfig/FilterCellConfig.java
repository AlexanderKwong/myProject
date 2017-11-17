package cellconfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class FilterCellConfig
{
	private static FilterCellConfig instance;

	public static FilterCellConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new FilterCellConfig();
		}
		return instance;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (!FilterCellConfig.GetInstance().loadFilterCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "filterCell init error please check !");
			throw (new IOException("filterCell init error  please check !"));
		}
	}

	private FilterCellConfig()
	{
		lteCellInfoList = new ArrayList<Long>();
	}

	////////////////////////////////////////////////// tdlte
	////////////////////////////////////////////////// ///////////////////////////////////////////////
	private ArrayList<Long> lteCellInfoList;

	public String errLog = "";

	public ArrayList<Long> getLteCellInfoList()
	{
		return lteCellInfoList;
	}

	public boolean loadFilterCell(Configuration conf)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getFilterCellConfigPath();
		return loadFilterCell(conf, filePath);
	}

	public boolean loadFilterCell(Configuration conf, String filePath)
	{
		try
		{
			BufferedReader reader = null;
			lteCellInfoList = new ArrayList<>();
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
				long eci;
				while ((strData = reader.readLine()) != null)
				{
					if (strData.trim().length() == 0)
					{
						continue;
					}
					try
					{
						eci = Long.parseLong(strData);
						if (lteCellInfoList.contains(eci))
						{
							continue;
						}
						lteCellInfoList.add(eci);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error : " + strData, e);
						errLog = "loadFilterCell error : " + e.getMessage() + ":" + strData;
						return false;
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error ", e);
				errLog = "loadFilterCell error : " + e.getMessage();
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
			LOGHelper.GetLogger().writeLog(LogType.error, "loadFilterCell error ", e);
			errLog = "loadFilterCell error : " + e.getMessage();
			return false;
		}

		return true;
	}

	public boolean getLteCell(long eci)
	{
		if (lteCellInfoList.contains(eci) || lteCellInfoList.size() == 0)
		{
			return true;
		}
		return false;
	}
}
