package mro.lablefill;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import StructData.StaticConfig;

public class MroLableMapper_FMT
{
	public static class MroDataMapper_FMT extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private String eci = "";
		private String beginTime = "";
		private int rsrp = 0;
		private final int TimeSpan = 600;// 10分钟间隔

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\\|" + "|" + "\t", -1);

			if (valstrs.length < 12)
			{
				return;
			}

			beginTime = valstrs[2];
			eci = valstrs[11];
			rsrp = Integer.parseInt(valstrs[19]);

			if (eci.length() <= 1 || beginTime.length() <= 1 || rsrp == -141)
			{
				return;
			}

			try
			{
				CellTimeKey keyItem = new CellTimeKey(Long.parseLong(eci), Integer.parseInt(beginTime) / TimeSpan * TimeSpan, 2);
				context.write(keyItem, value);
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}

		}
	}
}
