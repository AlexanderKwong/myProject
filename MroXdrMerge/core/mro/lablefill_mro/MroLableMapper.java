package mro.lablefill_mro;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mro.lablefill.CellTimeKey;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class MroLableMapper
{
	public static class XdrLocationMappers extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private long eci = 0;
		private int time = 0;
		private final int TimeSpan = 600;// 10分钟间隔
		private int sTimeSpan = 0;
		private int eTimeSpan = 0;
		
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			final Log LOG = LogFactory.getLog(XdrLocationMappers.class);
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

	public static class MroDataMappers extends Mapper<Object, Text, CellTimeKey, Text>
	{
		private String eci = "";
		private String beginTimes = "";
		private String beginTime = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private Date parseDate;
		private String tmCellID;
		private String tmENBId;
		protected static final Log LOG = LogFactory.getLog(MroDataMappers.class);
		SimpleDateFormat si = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			
		    parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if(parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
		    dataAdapterReader = new DataAdapterReader(parseItem);
		    
		    splitMax = parseItem.getSplitMax("MmeUeS1apId,beginTime,CellId,EventType");
		    if(splitMax < 0)
		    {
		    	throw new IOException("time or imsi pos not right.");
		    }
		}
		
		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);
		    
		    if (valstrs.length < 12)
			{
				return;
			}
		    
		    try
			{
		    	dataAdapterReader.readData(valstrs);		    	
		    	parseDate = dataAdapterReader.GetDateValue("beginTime", null);
				long time = parseDate.getTime()/ 1000L;
				beginTime = String.valueOf(time);
			
				tmCellID = dataAdapterReader.GetStrValue("CellId", "");
				tmENBId = dataAdapterReader.GetStrValue("ENBId", "");
				
				eci =String.valueOf((Integer.parseInt(tmENBId)*256+Integer.parseInt(tmCellID)));								

				if (eci.length() <= 1 || beginTime.length() <= 1)
				{
					return;
				}
				CellTimeKey keyItem = new CellTimeKey(Long.parseLong(eci),
						Integer.parseInt(beginTime) / TimeSpan * TimeSpan, 2);

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
			// return Math.abs((int)(key.getEci() & 0x00000000ffffffffL)) %
			// numOfReducer;
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
