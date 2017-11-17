package xdr.lablefill.by23g;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import StructData.StaticConfig;

public class XdrLableMapper
{

	public static class XdrDataMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String beginTime = "";
		private String imsi = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private String xmString = "";
		private String[] valstrs;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split("\t", 16);	

			if (valstrs[0].toLowerCase().equals("online_id"))
			{
				return;
			}

			if (valstrs.length < 15)
			{
				return;
			}

			beginTime = valstrs[14];
			imsi = valstrs[3].trim();

			if (beginTime.length() == 0 || imsi.trim().length() != 15)
			{
				return;
			}

			stime = 0;
			try
			{
				d_beginTime = format.parse(beginTime);
				stime = (int) (d_beginTime.getTime() / 1000L);
				
				Long.parseLong(imsi);
			}
			catch (Exception e)
			{
				return;
			}
			
			imsiKey = new ImsiTimeKey(Long.parseLong(imsi), stime, stime / TimeSpan * TimeSpan, 3);
			context.write(imsiKey, value);
		}
			
		
	}

	public static class ImsiPartitioner extends Partitioner<ImsiTimeKey, Text>implements Configurable
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
		public int getPartition(ImsiTimeKey key, Text value, int numOfReducer)
		{
			return Math.abs(String.valueOf(key.getImsi()).hashCode()) % numOfReducer;
		}

	}

	public static class ImsiSortKeyComparator extends WritableComparator
	{
		public ImsiSortKeyComparator()
		{
			super(ImsiTimeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			ImsiTimeKey s1 = (ImsiTimeKey) a;
			ImsiTimeKey s2 = (ImsiTimeKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class ImsiSortKeyGroupComparator extends WritableComparator
	{

		public ImsiSortKeyGroupComparator()
		{
			super(ImsiTimeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			ImsiTimeKey s1 = (ImsiTimeKey) a;
			ImsiTimeKey s2 = (ImsiTimeKey) b;

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
				if(s1.getTimeSpan() >  s2.getTimeSpan())
				{
					return 1;
				}
				else if(s1.getTimeSpan() <  s2.getTimeSpan())
				{
					return -1;
				}
				else 
				{
					if (s1.getDataType() > s2.getDataType())
					{
						return 1;
					}
					else if (s1.getDataType() < s2.getDataType())
					{
						return -1;
					}
					return 0;
				}
				
			}

		}

	}

}
