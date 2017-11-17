package mro.lablefillex_uemro;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mro.lablefill.CellTimeKey;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.lablefill.UEExclude;

public class UEMroLableMapper
{
	public static class XdrLocationMappers extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔
		private int sTimeSpan = 0;
		private int eTimeSpan = 0;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			String[] valstrs = value.toString().split("\t", -1);

			if (valstrs.length < 11)
			{
				return;
			}

			eci = Long.parseLong(valstrs[1].trim());
			time = Integer.parseInt(valstrs[3].trim());

			int tmTime = time / TimeSpan * TimeSpan;
			CellTimeKey keyItem = new CellTimeKey(eci, tmTime - TimeSpan, 1);
			context.write(keyItem, value);

			keyItem = new CellTimeKey(eci, tmTime, 1);
			context.write(keyItem, value);

			keyItem = new CellTimeKey(eci, tmTime + TimeSpan, 1);
			context.write(keyItem, value);
		}

	}

	public static class MroDataMappers extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private String beginTime = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private Date tmDate;
		private ParseItem parseItem;
		private int tmCellID;
		private int tmENBId;
		private DataAdapterReader dataAdapterReader;
		protected static final Log LOG = LogFactory.getLog(MroDataMappers.class);
		SimpleDateFormat si = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		int splitMax;
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
		    super.setup(context);
			
			UEExclude.GetInstance().loadData();
			
			//load xdr prepare data
			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
			
		    parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-UE");
			if(parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
		    dataAdapterReader = new DataAdapterReader(parseItem);
		    
		    splitMax = parseItem.getSplitMax("beginTime,CellId,ENBId");
		    if(splitMax < 0)
		    {
		    	throw new IOException("time or imsi pos not right.");
		    }
            ////////////////////	    
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax+2);
			dataAdapterReader.readData(valstrs);

			try
			{
				dataAdapterReader.readData(valstrs);
				tmDate = dataAdapterReader.GetDateValue("beginTime", null);
				beginTime = String.valueOf(tmDate.getTime() / 1000L);

				tmCellID = dataAdapterReader.GetIntValue("CellId", -1);
				tmENBId = dataAdapterReader.GetIntValue("ENBId", -1);
				eci = dataAdapterReader.GetLongValue("ECI", -1);
				
				if(eci > 0)
				{
					tmCellID = (int)(eci %256);
					tmENBId = (int)(eci /256);
				}
				else 
				{
					eci = tmENBId*256+tmCellID;
				}

				if (eci <= 0 || beginTime.length() <= 1)
				{
					return;
				}
				CellTimeKey keyItem = new CellTimeKey(eci,Integer.parseInt(beginTime) / TimeSpan * TimeSpan, 2);

				context.write(keyItem, value);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "map data error : " + value.toString(), e);
					return;
				}
			}
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
			return Math.abs(String.valueOf(key.getEci() + "_" + key.getTimeSpan()).hashCode()) % numOfReducer;
		}

	}

	public static class CellSortKeyComparator extends WritableComparator
	{
		public CellSortKeyComparator()
		{
			super(CellTimeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			CellTimeKey s1 = (CellTimeKey) a;
			CellTimeKey s2 = (CellTimeKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class CellSortKeyGroupComparator extends WritableComparator
	{

		public CellSortKeyGroupComparator()
		{
			super(CellTimeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			CellTimeKey s1 = (CellTimeKey) a;
			CellTimeKey s2 = (CellTimeKey) b;

			if (s1.getEci() > s2.getEci())
			{
				return 1;
			}
			else if (s1.getEci() < s2.getEci())
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
