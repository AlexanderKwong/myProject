package mro.format_mt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.DataGeter;

public class MroFormatMapper
{
	public static class MroMapper extends DataDealMapper<Object, Text, Text, Text>
	{
		private String MmeUeS1apId = "";
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String EventType = "";
		private long tmpLong;
		private String tmCellID;
		private int tmEnbid;

		private Text keyText = new Text();

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;

		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("MmeUeS1apId,beginTime,CellId,EventType");
			if (splitMax < 0)
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
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

			try
			{
				dataAdapterReader.readData(valstrs);
				MmeUeS1apId = dataAdapterReader.GetStrValue("MmeUeS1apId", "");
				TimeStamp = dataAdapterReader.GetDateValue("beginTime", null);
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				tmEnbid = dataAdapterReader.GetIntValue("ENBId", 0);
				if (tmCellID.indexOf(":") > 0)
				{
					tmCellID = tmCellID.substring(0, tmCellID.indexOf(":"));
				}
				else if (tmCellID.indexOf("-") > 0)
				{
					tmCellID = tmCellID.substring(tmCellID.indexOf("-") + 1);
				}
				if (Integer.parseInt(tmCellID) < 256)
				{
					tmCellID = (tmEnbid * 256 + Integer.parseInt(tmCellID)) + "";
				}

				EventType = dataAdapterReader.GetStrValue("EventType", "");

				keyText.set(tmCellID + MmeUeS1apId + dateFormat.format(TimeStamp) + EventType);
				context.write(keyText, value);
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
