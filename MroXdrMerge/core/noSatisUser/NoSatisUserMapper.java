package noSatisUser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mro.lablefill.CellTimeKey;

public class NoSatisUserMapper
{
	private static SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static class XdrLocationMappers extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 60分钟间隔
		public static final String spliter = "\t";

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			String[] valstrs = value.toString().split(spliter, -1);
			if (valstrs.length < 11)
			{
				return;
			}
			eci = Long.parseLong(valstrs[1].trim());
			time = Integer.parseInt(valstrs[3].trim());

			int tmTime = time / TimeSpan * TimeSpan;
			CellTimeKey keyItem = new CellTimeKey(eci, tmTime, 0);
			context.write(keyItem, value);
		}

	}

	public static class GMosMapper extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 60分钟间隔
		public static final String spliter = ",";

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			String[] valstrs = value.toString().split(spliter, -1);
			if (valstrs.length < 57)
			{
				return;
			}
			try
			{
				eci = Util.getEci(valstrs[14]);
				if (eci < 0)
				{
					return;
				}
				time = (int) (sf.parse(valstrs[0]).getTime() / 1000);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "GMosMapper map error", e);
			}

			int tmTime = time / TimeSpan * TimeSpan;
			CellTimeKey keyItem = new CellTimeKey(eci, tmTime, 1);
			context.write(keyItem, value);

		}
	}

	public static class WjtdhMapper extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 60分钟间隔
		public static final String spliter = ",";

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			String[] valstrs = value.toString().split(spliter, -1);
			if (valstrs.length < 13)
			{
				return;
			}
			try
			{
				eci = Util.getEci(valstrs[3]);
				if (eci < 0)
				{
					return;
				}
				time = (int) (sf.parse(valstrs[1]).getTime() / 1000);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "WjtdhMapper map error", e);
			}

			int tmTime = time / TimeSpan * TimeSpan;
			CellTimeKey keyItem = new CellTimeKey(eci, tmTime, 2);
			context.write(keyItem, value);
		}
	}

	public static class CellPartitioner extends Partitioner<CellTimeKey, Text> implements Configurable
	{
		private Configuration conf = null;

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public void setConf(Configuration conf)
		{
			this.conf = conf;
		}

		@Override
		public int getPartition(CellTimeKey key, Text value, int numOfReducer)
		{
			return Math.abs(String.valueOf(key.getEci()).hashCode()) % numOfReducer;
		}
	}
}
