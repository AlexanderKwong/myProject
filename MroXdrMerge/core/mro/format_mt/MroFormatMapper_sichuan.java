package mro.format_mt;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class MroFormatMapper_sichuan
{
	public static class MroMapper_ERICSSON extends DataDealMapper<Object, Text, Text, Text>
	{
		private String MmeUeS1apId = "";
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String EventType = "-1";
		private String tmCellID;

		private Text keyText = new Text();
		private Text Mt_value = new Text();

		private ParseItem parseItem_fdd;
		private DataAdapterReader dataAdapterReader_fdd;
		private int splitMax_fdd = -1;

		private ParseItem parseItem_td;
		private DataAdapterReader dataAdapterReader_td;
		private int splitMax_td = -1;

		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem_fdd = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-ERICSSON-FDD");
			if (parseItem_fdd == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader_fdd = new DataAdapterReader(parseItem_fdd);

			splitMax_fdd = parseItem_fdd.getSplitMax("MmeUeS1apId,beginTime,CellId");
			if (splitMax_fdd < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}

			parseItem_td = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-ERICSSON-TD");
			if (parseItem_td == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader_td = new DataAdapterReader(parseItem_td);

			splitMax_td = parseItem_td.getSplitMax("MmeUeS1apId,beginTime,CellId");
			if (splitMax_td < 0)
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
			String[] valstrs = xmString.toString().split(",|\t", -1);

			try
			{
				if (valstrs.length > 50)
				{
					dataAdapterReader_fdd.readData(valstrs);
					if (!dataAdapterReader_fdd.GetStrValue("measurementtype", "").equals("M1"))
					{
						return;
					}
					MmeUeS1apId = dataAdapterReader_fdd.GetStrValue("MmeUeS1apId", "");
					String timeTemp = dataAdapterReader_fdd.GetStrValue("beginTime", null);
					TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
					tmCellID = dataAdapterReader_fdd.GetStrValue("CellId", "0");
					String MT_mro = formatERICSSON_FDD(dataAdapterReader_fdd);
					if (MT_mro.length() == 0)
					{
						return;
					}
					Mt_value.set(MT_mro);
				}
				else
				{
					dataAdapterReader_td.readData(valstrs);
					if (!dataAdapterReader_td.GetStrValue("measurementtype", "").equals("M1"))
					{
						return;
					}
					MmeUeS1apId = dataAdapterReader_td.GetStrValue("MmeUeS1apId", "");
					String timeTemp = dataAdapterReader_td.GetStrValue("beginTime", null);
					TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
					tmCellID = dataAdapterReader_td.GetStrValue("CellId", "0");
					String MT_mro = formatERICSSON_TD(dataAdapterReader_td);
					if (MT_mro.length() == 0)
					{
						return;
					}
					Mt_value.set(MT_mro);
				}
				keyText.set(tmCellID + MmeUeS1apId + dateFormat.format(TimeStamp) + EventType);
				context.write(keyText, Mt_value);
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

		public String formatERICSSON_FDD(DataAdapterReader dataAdapterReader)
		{
			StringBuffer MTMroString = new StringBuffer();
			try
			{
				MTMroString.append(dataAdapterReader.GetStrValue("beginTime", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) / 256);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("userLabel", "userLable"));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) % 256);
				MTMroString.append(spliter);
				MTMroString.append(-1);
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeCode", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeGroupId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
				MTMroString.append(spliter);
				MTMroString.append("0");
				MTMroString.append(spliter);
				MTMroString.append("MRO");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellCarrierRSSI", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcch", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellNcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsPccpchRSCP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsNcellUarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsCellParameterId", ""));
				MTMroString.append(spliter);
				MTMroString.append(0);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRTTD", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScTadv", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScAOA", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPHR", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScSinrUL", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI1", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI2", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI4", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI8", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPUSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPDSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteSceNBRxTxTimeDiff", ""));
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				return "";
			}
			return MTMroString.toString();
		}

		public String formatERICSSON_TD(DataAdapterReader dataAdapterReader)
		{
			StringBuffer MTMroString = new StringBuffer();
			try
			{
				MTMroString.append(dataAdapterReader.GetStrValue("beginTime", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) / 256);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("userLabel", "userLable"));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) % 256);
				MTMroString.append(spliter);
				MTMroString.append(-1);
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeCode", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeGroupId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
				MTMroString.append(spliter);
				MTMroString.append("0");
				MTMroString.append(spliter);
				MTMroString.append("-1");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellCarrierRSSI", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcch", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellNcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsPccpchRSCP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsNcellUarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsCellParameterId", ""));
				MTMroString.append(spliter);
				MTMroString.append(0);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRTTD", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScTadv", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScAOA", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPHR", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScSinrUL", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI1", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI2", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI4", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI8", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPUSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPDSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteSceNBRxTxTimeDiff", ""));
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				return "";
			}
			return MTMroString.toString();
		}
	}

	public static class MroMapper_HUAWEI_TD extends DataDealMapper<Object, Text, Text, Text>
	{
		private String MmeUeS1apId = "";
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String EventType = "-1";
		private String tmCellID;

		private Text keyText = new Text();
		private Text Mt_value = new Text();

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;

		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-HUAWEI-TD");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("MmeUeS1apId,beginTime,CellId");
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
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				if (!dataAdapterReader.GetStrValue("measurementtype", "").equals("M1"))
				{
					return;
				}
				MmeUeS1apId = dataAdapterReader.GetStrValue("MmeUeS1apId", "");
				String timeTemp = dataAdapterReader.GetStrValue("beginTime", null);
				TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " ")); 
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				keyText.set(tmCellID + MmeUeS1apId + dateFormat.format(TimeStamp) + EventType);
				String MT_mro = formatHUAWEI_TD(dataAdapterReader);
				if (MT_mro.length() == 0)
				{
					return;
				}
				Mt_value.set(MT_mro);
				context.write(keyText, Mt_value);
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

		public String formatHUAWEI_TD(DataAdapterReader dataAdapterReader)
		{
			StringBuffer MTMroString = new StringBuffer();
			try
			{
				MTMroString.append(dataAdapterReader.GetStrValue("beginTime", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) / 256);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("userLabel", "userLable"));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) % 256);
				MTMroString.append(spliter);
				MTMroString.append(-1);
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeCode", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeGroupId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
				MTMroString.append(spliter);
				MTMroString.append("0");
				MTMroString.append(spliter);
				MTMroString.append("MRO");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellCarrierRSSI", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcch", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellNcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsPccpchRSCP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsNcellUarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsCellParameterId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.MR.LteScBSR", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRTTD", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScTadv", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScAOA", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPHR", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScSinrUL", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI1", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI2", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI4", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI8", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPUSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPDSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteSceNBRxTxTimeDiff", ""));
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				return "";
			}
			return MTMroString.toString();
		}
	}

	public static class MroMapper_NSN_TD extends DataDealMapper<Object, Text, Text, Text>
	{
		private String MmeUeS1apId = "";
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String EventType = "-1";
		private String tmCellID;

		private Text keyText = new Text();
		private Text Mt_value = new Text();

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;

		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-NSN-TD");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("MmeUeS1apId,beginTime,CellId");
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
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				if (!dataAdapterReader.GetStrValue("measurementtype", "").equals("M1"))
				{
					return;
				}
				MmeUeS1apId = dataAdapterReader.GetStrValue("MmeUeS1apId", "");
				String timeTemp = dataAdapterReader.GetStrValue("beginTime", null);
				TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				keyText.set(tmCellID + MmeUeS1apId + dateFormat.format(TimeStamp) + EventType);
				String MT_mro = formatNSN_TD(dataAdapterReader);
				if (MT_mro.length() == 0)
				{
					return;
				}
				Mt_value.set(MT_mro);
				context.write(keyText, Mt_value);
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

		public String formatNSN_TD(DataAdapterReader dataAdapterReader)
		{
			StringBuffer MTMroString = new StringBuffer();
			try
			{
				MTMroString.append(dataAdapterReader.GetStrValue("beginTime", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) / 256);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("userLabel", "userLable"));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) % 256);
				MTMroString.append(spliter);
				MTMroString.append(-1);
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeCode", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeGroupId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
				MTMroString.append(spliter);
				MTMroString.append("0");
				MTMroString.append(spliter);
				MTMroString.append("MRO");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellCarrierRSSI", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcch", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellNcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsPccpchRSCP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsNcellUarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsCellParameterId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.MR.LteScBSR", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRTTD", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScTadv", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScAOA", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPHR", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScSinrUL", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI1", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI2", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI4", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI8", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPUSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPDSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteSceNBRxTxTimeDiff", ""));
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				return "";
			}
			return MTMroString.toString();
		}
	}

	public static class MroMapper_ZTE_TD extends DataDealMapper<Object, Text, Text, Text>
	{
		private String MmeUeS1apId = "";
		private Date TimeStamp;// 2015-11-01 00:02:43.000
		private String EventType = "-1";
		private String tmCellID;

		private Text keyText = new Text();
		private Text Mt_value = new Text();

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;

		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public static String spliter = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-ZTE-TD");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("MmeUeS1apId,beginTime,CellId");
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
			String[] valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);

			try
			{
				dataAdapterReader.readData(valstrs);
				if (!dataAdapterReader.GetStrValue("measurementtype", "").equals("M1"))
				{
					return;
				}
				MmeUeS1apId = dataAdapterReader.GetStrValue("MmeUeS1apId", "");
				String timeTemp = dataAdapterReader.GetStrValue("beginTime", null);
				TimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(timeTemp.replace("T", " "));
				tmCellID = dataAdapterReader.GetStrValue("CellId", "0");
				keyText.set(tmCellID + MmeUeS1apId + dateFormat.format(TimeStamp) + EventType);
				String MT_mro = formatZTE_TD(dataAdapterReader);
				if (MT_mro.length() == 0)
				{
					return;
				}
				Mt_value.set(MT_mro);
				context.write(keyText, Mt_value);
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

		public String formatZTE_TD(DataAdapterReader dataAdapterReader)
		{
			StringBuffer MTMroString = new StringBuffer();
			try
			{
				MTMroString.append(dataAdapterReader.GetStrValue("beginTime", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) / 256);
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("userLabel", "userLable"));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetIntValue("CellId", 1) % 256);
				MTMroString.append(spliter);
				MTMroString.append(-1);
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeCode", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeGroupId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MmeUeS1apId", ""));
				MTMroString.append(spliter);
				MTMroString.append("0");
				MTMroString.append(spliter);
				MTMroString.append("MRO");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcRSRQ", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcEarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteNcPci", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellCarrierRSSI", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcch", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellNcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.GsmNcellBcc", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsPccpchRSCP", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsNcellUarfcn", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.TdsCellParameterId", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.MR.LteScBSR", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRTTD", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScTadv", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScAOA", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPHR", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScSinrUL", ""));
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append("");
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI1", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI2", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI4", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScRI8", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPUSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteScPDSCHPRBNum", ""));
				MTMroString.append(spliter);
				MTMroString.append(dataAdapterReader.GetStrValue("MR.LteSceNBRxTxTimeDiff", ""));
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				return "";
			}
			return MTMroString.toString();
		}
	}

}
