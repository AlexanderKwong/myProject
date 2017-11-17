package mro.lablefillex;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Text;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mro.lablefill.CellTimeKey;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class MdtDataMapper
{
	public static long GetEci(String tmCellID)
	{
		long eci = 0;
		if (tmCellID.indexOf("-") > 0)
		{
			String enbid = tmCellID.substring(0, tmCellID.indexOf("-"));
			String cellid = tmCellID.substring(tmCellID.indexOf("-") + 1);
			eci = Long.parseLong(enbid) * 256 + Long.parseLong(cellid);
		}
		else
		{
			eci = Long.parseLong(tmCellID);
		}
		return eci;
	}

	public static class MdtImmMapper extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;
		private int TimeSpan = 600;

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private String enbid;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-IMM");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "map data too long : " + value.toString());
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				TimeStamp = dataAdapterReader.GetDateValue("beginTime", null);
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				enbid = dataAdapterReader.GetStrValue("ENBId", "0");
				long eci = GetEci(tmCellID);
				if (eci < 256)
		        {
		          eci = Integer.parseInt(enbid) * 256 + eci;
		        }
				CellTimeKey keyItem = new CellTimeKey(eci, (int) (TimeStamp.getTime() / 1000 + 8 * 3600) / TimeSpan * TimeSpan, 3);
				try
				{
					context.write(keyItem, value);
				}
				catch (Exception e)
				{
					// TODO Auto-generated catch block
				}
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

	public static class MdtLogMapper extends DataDealMapper<Object, Text, CellTimeKey, Text>
	{
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String tmCellID;
		private int TimeSpan = 600;

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private String enbid;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-LOG");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().length() == 0)
			{
				return;
			}

			if (value.toString().length() >= 100000)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "map data too long : " + value.toString());
				return;
			}

			String xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				TimeStamp = dataAdapterReader.GetDateValue("beginTime", null);
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				enbid = dataAdapterReader.GetStrValue("ENBId", "0");
				long eci = GetEci(tmCellID);
				if (eci < 256)
		        {
		          eci = Integer.parseInt(enbid) * 256 + eci;
		        }
				CellTimeKey keyItem = new CellTimeKey(eci, (int) (TimeStamp.getTime() / 1000 + 8 * 3600) / TimeSpan * TimeSpan, 4);
				try
				{
					context.write(keyItem, value);
				}
				catch (Exception e)
				{
					// TODO Auto-generated catch block
				}
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

}
