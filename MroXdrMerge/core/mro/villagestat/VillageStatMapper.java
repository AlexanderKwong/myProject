package mro.villagestat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import StructData.GridItem;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.LOGHelper;
import jan.util.data.MyInt;
import util.DataGeter;
import util.MrLocation;
import util.MrLocationItem;

public class VillageStatMapper
{
	public static class VillageGridMapper extends DataDealMapper<Object, Text, GridTypeKey, Text>
	{
		private int tllong = 0;
		private int tllat = 0;
		private String xmString = "";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.split("\t", -1);

			if (valstrs.length < 3)
			{
				return;
			}

			try
			{
				tllong = Integer.parseInt(valstrs[0]);
				tllat = Integer.parseInt(valstrs[1]);
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}

			GridTypeKey gridTypeKey = new GridTypeKey(tllong, tllat, 1);
			context.write(gridTypeKey, value);
		}

	}

	public static class MroDataMapper extends DataDealMapper<Object, Text, GridTypeKey, Text>
	{
		private long eci;
		private int rttd;
		private int tadv;
		private int aoa;
		private String valueStr = "";
		private Text resText = new Text();
		
		private Map<String, MyInt> tempMap;
		private int maxInt = -1;
		private int tmpLong = -1;
	    private int tmpLat = -1;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			
			// 初始化lte小区的信息
			if(!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！");
				throw(new IOException("ltecell init error 请检查！"));
			}
			
			tempMap = new HashMap<String, MyInt>();
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			LOGHelper.GetLogger().writeLog(LogType.info,  "max location: " + "\t" + tmpLong + "\t" + tmpLat + "\t" + maxInt);
			
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\t", 30);
			
			if (valstrs.length < 28)
			{
				return;
			}
			
			try
			{
				eci = DataGeter.GetLong(valstrs[11]);
				rttd = DataGeter.GetInt(valstrs[24]);
				tadv = DataGeter.GetInt(valstrs[25]);
				aoa = DataGeter.GetInt(valstrs[26]);
			}
			catch (Exception e)
			{
				return;
			}
			
			if(eci < 0)
				return;

			LteCellInfo lteCell = CellConfig.GetInstance().getLteCell(eci);
			if(lteCell == null)
			{
//				LOGHelper.GetLogger().writeLog(LogType.info,  "cell not found" + "\t" + value.toString());
				return;
			}			

			MrLocationItem location = MrLocation.calcLongLac(tadv, rttd, aoa, lteCell.ilongitude / 10000000.0, lteCell.ilatitude / 10000000.0, lteCell.indoor == 1? true:false, lteCell.angle, true);
            if(location == null)
            {
//            	LOGHelper.GetLogger().writeLog(LogType.info,  "get location error" + "\t" + value.toString());
            	return;
            }
            
            if(location.longitude <= 0 || location.latitude <= 0)
            {
            	LOGHelper.GetLogger().writeLog(LogType.info,  "get location error1" + "\t" + location.longitude + "\t" + location.latitude);
            	return;
            }
            
//            if(location.longitude > 0)
//            {
//            	LOGHelper.GetLogger().writeLog(LogType.info, location.longitude + "\t" + location.latitude + "\t" + value.toString());
//            }
			
			GridItem gridItem = GridItem.GetGridItem(0, location.longitude, location.latitude);
            if(gridItem.getTLLongitude() <= 0 || gridItem.getTLLatitude() <= 0)
            {
            	LOGHelper.GetLogger().writeLog(LogType.info,  "get location error2" + "\t" + gridItem.getTLLongitude() + "\t" + gridItem.getTLLatitude());
            	return;
            }
			
			
			GridTypeKey keyItem = new GridTypeKey(gridItem.getTLLongitude(), gridItem.getTLLatitude(), 100);
			
			valueStr =  lteCell.cityid + StaticConfig.DataSliper2 + location.longitude + StaticConfig.DataSliper2 + location.latitude + StaticConfig.DataSliper2 + value.toString();
			resText.set(valueStr);	
			
			///////////////////////////////////////////////////////////////////////////
			MyInt tmpInt = tempMap.get(keyItem.getTllong() + "_" + keyItem.getTllat());
			if(tmpInt == null)
			{
				tmpInt = new MyInt(0);
				tempMap.put(keyItem.getTllong() + "_" + keyItem.getTllat(), tmpInt);
			}
			tmpInt.data++;
			if(maxInt < tmpInt.data)
			{
				maxInt = tmpInt.data;
				tmpLong = keyItem.getTllong();
				tmpLat = keyItem.getTllat();
			}
			
			if(keyItem.getTllong() == 2147480000)
			{
				LOGHelper.GetLogger().writeLog(LogType.info,  "get location" + "\t" + keyItem.getTllong() + "\t" + keyItem.getTllat() + "\t" + lteCell.cellid + "\t" + lteCell.ilongitude + "\t" + lteCell.ilatitude + "\t" + lteCell.angle + "\t" + rttd + "\t" + tadv + "\t" + aoa + "\t" + location.longitude + "\t" + location.latitude);
			}
			
			context.write(keyItem, resText);
		}

	}

	public static class MroDataMapper_UEMR extends DataDealMapper<Object, Text, GridTypeKey, Text>
	{
		private long eci;
		private int rttd;
		private int tadv;
		private int aoa;
		private String valueStr = "";
		private Text resText = new Text();
		
		private Map<String, MyInt> tempMap;
		private int maxInt = -1;
		private int tmpLong = -1;
	    private int tmpLat = -1;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			
			// 初始化lte小区的信息
			if(!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！");
				throw(new IOException("ltecell init error 请检查！"));
			}
			
			tempMap = new HashMap<String, MyInt>();
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			LOGHelper.GetLogger().writeLog(LogType.info,  "max location: " + "\t" + tmpLong + "\t" + tmpLat + "\t" + maxInt);
			
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\t", 30);
			
			if (valstrs.length < 28)
			{
				return;
			}
			
			try
			{
				eci = DataGeter.GetLong(valstrs[11]);
				rttd = DataGeter.GetInt(valstrs[24]);
				tadv = DataGeter.GetInt(valstrs[25]);
				aoa = DataGeter.GetInt(valstrs[26]);
			}
			catch (Exception e)
			{
				return;
			}
			
			if(eci < 0)
				return;

			LteCellInfo lteCell = CellConfig.GetInstance().getLteCell(eci);
			if(lteCell == null)
			{
//				LOGHelper.GetLogger().writeLog(LogType.info,  "cell not found" + "\t" + value.toString());
				return;
			}			

			MrLocationItem location = MrLocation.calcLongLac(tadv, rttd, aoa, lteCell.ilongitude / 10000000.0, lteCell.ilatitude / 10000000.0, lteCell.indoor == 1? true:false, lteCell.angle, true);
            if(location == null)
            {
//            	LOGHelper.GetLogger().writeLog(LogType.info,  "get location error" + "\t" + value.toString());
            	return;
            }
            
            if(location.longitude <= 0 || location.latitude <= 0)
            {
            	LOGHelper.GetLogger().writeLog(LogType.info,  "get location error1" + "\t" + location.longitude + "\t" + location.latitude);
            	return;
            }
            
//            if(location.longitude > 0)
//            {
//            	LOGHelper.GetLogger().writeLog(LogType.info, location.longitude + "\t" + location.latitude + "\t" + value.toString());
//            }
			
			GridItem gridItem = GridItem.GetGridItem(0, location.longitude, location.latitude);
            if(gridItem.getTLLongitude() <= 0 || gridItem.getTLLatitude() <= 0)
            {
            	LOGHelper.GetLogger().writeLog(LogType.info,  "get location error2" + "\t" + gridItem.getTLLongitude() + "\t" + gridItem.getTLLatitude());
            	return;
            }
			
			
			GridTypeKey keyItem = new GridTypeKey(gridItem.getTLLongitude(), gridItem.getTLLatitude(), 100);
			
			valueStr =  lteCell.cityid + StaticConfig.DataSliper2 + location.longitude + StaticConfig.DataSliper2 + location.latitude + StaticConfig.DataSliper2 + value.toString();
			resText.set(valueStr);	
			
			///////////////////////////////////////////////////////////////////////////
			MyInt tmpInt = tempMap.get(keyItem.getTllong() + "_" + keyItem.getTllat());
			if(tmpInt == null)
			{
				tmpInt = new MyInt(0);
				tempMap.put(keyItem.getTllong() + "_" + keyItem.getTllat(), tmpInt);
			}
			tmpInt.data++;
			if(maxInt < tmpInt.data)
			{
				maxInt = tmpInt.data;
				tmpLong = keyItem.getTllong();
				tmpLat = keyItem.getTllat();
			}
			
			if(keyItem.getTllong() == 2147480000)
			{
				LOGHelper.GetLogger().writeLog(LogType.info,  "get location" + "\t" + keyItem.getTllong() + "\t" + keyItem.getTllat() + "\t" + lteCell.cellid + "\t" + lteCell.ilongitude + "\t" + lteCell.ilatitude + "\t" + lteCell.angle + "\t" + rttd + "\t" + tadv + "\t" + aoa + "\t" + location.longitude + "\t" + location.latitude);
			}
			
			context.write(keyItem, resText);
		}

	}

	
	
	public static class GridPartitioner extends Partitioner<GridTypeKey, Text> implements Configurable
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
		public int getPartition(GridTypeKey key, Text value, int numOfReducer)
		{
			// return Math.abs((int)(key.getEci() & 0x00000000ffffffffL)) %
			// numOfReducer;
			return Math.abs(String.valueOf(key.getTllong() + "_" + key.getTllat()).hashCode()) % numOfReducer;
		}

	}

	public static class GridSortKeyComparator extends WritableComparator
	{
		public GridSortKeyComparator()
		{
			super(GridTypeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			GridTypeKey s1 = (GridTypeKey) a;
			GridTypeKey s2 = (GridTypeKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class GridSortKeyGroupComparator extends WritableComparator
	{

		public GridSortKeyGroupComparator()
		{
			super(GridTypeKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			GridTypeKey s1 = (GridTypeKey) a;
			GridTypeKey s2 = (GridTypeKey) b;

			if (s1.getTllong() > s2.getTllong())
			{
				return 1;
			}
			else if (s1.getTllong() < s2.getTllong())
			{
				return -1;
			}
			else
			{
				if (s1.getTllat() > s2.getTllat())
				{
					return 1;
				}
				else if (s1.getTllat() < s2.getTllat())
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
					else
					{
						return 0;
					}
				}
			}

		}

	}

}
