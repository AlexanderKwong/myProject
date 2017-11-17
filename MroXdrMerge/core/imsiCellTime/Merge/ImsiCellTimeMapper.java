package imsiCellTime.Merge;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import mdtstat.Util;

public class ImsiCellTimeMapper
{
	public static class ImsiCellTimeMap extends Mapper<Object, Text, ImeiCellTimesKey, Text>
	{
		public ImeiCellTimesKey mapkey;
		public long imsi;
		public long eci;
		public long dayhour;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split(",", -1);
			if (strs.length < 6)
			{
				return;
			}
			imsi = Long.parseLong(strs[0]);
			dayhour = Util.getHour(Integer.parseInt(strs[1]));
			eci = Long.parseLong(strs[2]);
			mapkey = new ImeiCellTimesKey(imsi, dayhour, eci);
			context.write(mapkey, value);
		}

	}

	public static class ImsiPartitioner extends Partitioner<ImeiCellTimesKey, Text> implements Configurable
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
		public int getPartition(ImeiCellTimesKey key, Text text, int reduceNum)
		{
			// TODO Auto-generated method stub
			return Math.abs(("" + key.imsi).hashCode()) % reduceNum;
		}

	}

}
