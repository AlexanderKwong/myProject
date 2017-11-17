package locuser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import cellconfig.LteCellInfo;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class CfgSimuFile
{
	// private DistributedFileSystem hdfs;
	private HDFSOper hdfsOper = null;

	public CfgSimuFile()
	{
		Configuration conf = new Configuration();
		try
		{
			hdfsOper = new HDFSOper(conf);
			// hdfs = hdfsOper.getHdfs();
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			// hdfs = null;
		}
	}

	public void GetSimu(int eci, CfgInfo cf, CfgSimu osd)
	{
		int cityid = cf.GetCityid(eci);
		if (cityid < 0)
		{
			return;
		}

		GetSimuData(eci, 10, cf, cityid, osd);
		GetSimuData(eci, 40, cf, cityid, osd);
	}

	private void GetSimuData(int eci, int radius, CfgInfo cf, int cityid, CfgSimu osd)
	{
		String strPath = MainModel.GetInstance().getAppConfig().getFigureConfigPath() + "/" + String.valueOf(cityid) + "/" + String.valueOf(eci) + (radius == 40 ? "_40.txt" : ".txt");

		LOGHelper.GetLogger().writeLog(LogType.info, strPath);
		InputStream is = null;
		boolean iscomp = false;

		if (!strPath.contains(":"))
		{
			// Path fp = new Path(strPath);
			try
			{
				if (!hdfsOper.checkFileExist(strPath))
				{
					strPath += ".gz";
					// fp = new Path(strPath);
					if (!hdfsOper.checkFileExist(strPath))
					{
						return;
					}
					iscomp = true;
				}

				FileStatus fs = hdfsOper.getFileStatus(strPath);
				if (fs == null || fs.getLen() == 0)
				{
					return;
				}
				is = hdfsOper.getInputStream(strPath);
			}
			catch (Exception e)
			{
				// e.printStackTrace();
				return;
			}
		}
		else
		{
			iscomp = true;
			strPath += ".gz";
			try
			{
				is = new FileInputStream(strPath);
			}
			catch (FileNotFoundException e)
			{
				// e.printStackTrace();
				return;
			}
		}

		BufferedReader reader = null;
		try
		{
			if (iscomp)
			{
				reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is), "UTF-8"));
			}
			else
			{
				reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
			}
			String strData;
			String[] values;

			while ((strData = reader.readLine()) != null)
			{
				if (strData.length() == 0)
				{
					continue;
				}

				try
				{
					values = strData.split("\t", -1);

					int n = 0;
					int bid = Integer.parseInt(values[n++]);
					if (bid != -99)
					{
						GridData gd = new GridData();
						SimuData sd = new SimuData();
						gd.radius = radius;
						gd.bid = bid;

						if (values.length > 6)
						{// 旧格式
							gd.ilongitude = Integer.parseInt(values[n++]);
							gd.ilatitude = Integer.parseInt(values[n++]);
							gd.level = Integer.parseInt(values[n++]);
							sd.ieci = Integer.parseInt(values[n++]);
							sd.iearfcn = Integer.parseInt(values[n++]);
							sd.ipci = Integer.parseInt(values[n++]);
						}
						else
						{// 新格式
							sd.ieci = Integer.parseInt(values[n++]);
							gd.ilongitude = Integer.parseInt(values[n++]);
							gd.ilatitude = Integer.parseInt(values[n++]);
							gd.level = Integer.parseInt(values[n++]);

							LteCellInfo cellinfo = cf.lcconf.getLteCell(sd.ieci);
							if (cellinfo != null)
							{
								sd.iearfcn = cellinfo.fcn;
								sd.ipci = cellinfo.pci;
							}
						}
						sd.rsrp = Double.parseDouble(values[n++]);

						if (eci == sd.ieci)
						{
							sd.isscell = 1;
						}

						gd.scell = sd; // 借用传出去

						osd.SimuFallBack(gd);
					}
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "loadTdCell error : " + strData, e);
				}
			}
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "loadTdCell error ", e);
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
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
