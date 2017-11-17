package mro.lablefill_uemro;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealMapper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.DataGeter;

public class MroLableMapper
{
	
	public static class XdrLocationMapper extends DataDealMapper<Object, Text, ImsiKey, Text>
	{
		private long imsi = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String strValue = value.toString();
			String[] valstrs = strValue.split(StaticConfig.DataSliper2 + "|" + "\\|" + "|" + "\t", -1);

			if (valstrs.length < 11)
			{
				return;
			}
	
			imsi = DataGeter.GetLong(valstrs[1], 1);

			ImsiKey keyItem = new ImsiKey(imsi, 1);
			
			context.write(keyItem, value);
		}

	}

	public static class MroDataMapper extends DataDealMapper<Object, Text, ImsiKey, Text>
	{
		private long imsi = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.ShenZhen))
			{
				String[] valstrs = value.toString().split(",", -1);
				String tmImsi = valstrs[6].length()>15?valstrs[6].substring(0, 15):"0";
				imsi = Long.parseLong(tmImsi, 16);
				if (imsi <= 0)
				{
					return;
				}
					
				try
				{
					ImsiKey keyItem = new ImsiKey(imsi, 2);
					context.write(keyItem, value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}
			else 
			{
				String[] valstrs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\\|" + "|" + "\t", -1);

				if (valstrs.length < 12)
				{
					return;
				}

				imsi = DataGeter.GetLong(valstrs[5], 0);
				if (imsi <= 0)
				{
					return;
				}

				try
				{
					ImsiKey keyItem = new ImsiKey(imsi, 2);
					context.write(keyItem, value);
				}
				catch (Exception e)
				{
					// TODO: handle exception
				}
			}

		}

	}

	public static class CellPartitioner extends Partitioner<ImsiKey, Text> implements Configurable
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
		public int getPartition(ImsiKey key, Text value, int numOfReducer)
		{
			return Math.abs(String.valueOf(key.getImsi()).hashCode()) % numOfReducer;
		}

	}

	public static class CellSortKeyComparator extends WritableComparator
	{
		public CellSortKeyComparator()
		{
			super(ImsiKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			ImsiKey s1 = (ImsiKey) a;
			ImsiKey s2 = (ImsiKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class CellSortKeyGroupComparator extends WritableComparator
	{

		public CellSortKeyGroupComparator()
		{
			super(ImsiKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			ImsiKey s1 = (ImsiKey) a;
			ImsiKey s2 = (ImsiKey) b;

			if (s1.getImsi() > s2.getImsi())
			{
				return 1;
			}
			else if (s1.getImsi() < s2.getImsi())
			{
				return -1;
			}
			else
			{
				return 0;
			}

		}

	}

}
