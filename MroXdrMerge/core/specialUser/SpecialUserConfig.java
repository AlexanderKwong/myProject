package specialUser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mroxdrmerge.MainModel;
import util.StringUtil;

public class SpecialUserConfig
{
	private static SpecialUserConfig instance;
	private HashMap<Long, Integer> SpecialuserMap;

	private SpecialUserConfig()
	{
		SpecialuserMap = new HashMap<Long, Integer>();
	}

	public static SpecialUserConfig GetInstance()
	{
		if (instance == null)
		{
			instance = new SpecialUserConfig();
		}
		return instance;
	}

	/**
	 * 
	 * @param imsi
	 * @param EncryptUser
	 *            是否加密
	 * @return
	 */
	public boolean ifSpeciUser(Long imsi, boolean EncryptUser)
	{
		long key = 0L;
		if (EncryptUser)
		{
			key = StringUtil.EncryptStringToLong(imsi + "");
		}
		else
		{
			key = imsi;
		}
		if (SpecialuserMap.get(key) == null)
		{
			return false;
		}
		return true;
	}

	public static void main(String args[])
	{
		SpecialUserConfig.GetInstance().loadSpecialuser(null, false);
		for (Long imsi : SpecialUserConfig.GetInstance().SpecialuserMap.keySet())
		{
			System.out.println(imsi);
		}
		System.out.println(SpecialUserConfig.GetInstance().ifSpeciUser(12345678L, false));
	}

	/**
	 * 
	 * @param conf
	 * @param EncryptUser
	 *            是否加密
	 * @return
	 */
	public boolean loadSpecialuser(Configuration conf, boolean EncryptUser)
	{
		String filePath = MainModel.GetInstance().getAppConfig().getSpecialUserPath();
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
						LOGHelper.GetLogger().writeLog(LogType.error, "specialUserPath  is not exists: " + filePath);
						return false;
					}
					reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
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
					Long imsi = 0L;
					if (EncryptUser)
					{
						imsi = StringUtil.EncryptStringToLong(strData.trim());
					}
					else
					{
						imsi = Long.parseLong(strData.trim());

					}
					SpecialuserMap.put(imsi, 1);
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
			LOGHelper.GetLogger().writeLog(LogType.error, "specialUser  error ", e);
			return false;
		}
		return true;

	}

}
