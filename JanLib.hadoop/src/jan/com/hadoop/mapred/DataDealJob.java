package jan.com.hadoop.mapred;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;

public class DataDealJob
{
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");
	private Job job;
	private HDFSOper hdfsOper;

	public DataDealJob(Job job, HDFSOper hdfsOper)
	{
		this.job = job;
		this.hdfsOper = hdfsOper;
	}

	public boolean MergeSmallFile()
	{
		Configuration conf = job.getConfiguration();
		String mappers = conf.get("mapreduce.input.multipleinputs.dir.mappers");
		String[] mapper = mappers.split(",");
		for (String mm : mapper)
		{
			String[] items = mm.split(";");
			String path = items[0];
			if (!hdfsOper.mergeDirSmallFiles(path, "\n", 30 * 1024 * 1024))// 30M文件合并
			{
				return false;
			}
		}
		return true;
	}

	public boolean Work()
	{
		try
		{
			// if(!MergeSmallFile())
			// {
			// System.out.println("merge small file error!");
			// return false;
			// }

			Configuration conf = job.getConfiguration();

			Date stime = new Date();
			if (!job.waitForCompletion(true))
			{
				System.out.println("datadealjob error! stop run.");
				return false;
			}
			Date etime = new Date();

			if (conf.get("mapreduce.output.fileoutputformat.outputdir").length() > 0)
			{
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", conf.get("mapreduce.output.fileoutputformat.outputdir"), mins, timeFormat.format(stime), timeFormat.format(etime));
				if (hdfsOper == null)
				{
					new File(timeFileName).createNewFile();
				}
				else
				{
					hdfsOper.mkfile(timeFileName);
				}

			}

		}
		catch (Exception e)
		{
			
			LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
			return false;
		}
		return true;
	}

}
