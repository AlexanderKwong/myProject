package xdr.lablefill;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.FilterCellConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.FilterByEci;

public class XdrLableMapper
{
	public static final int DATATYPE_4G_MME = 1;
	public static final int DATATYPE_4G_HTTP = 2;
	public static final int DATATYPE_23G = 3;

	public static class ImsiCellTimeMap extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String imsi = "";
		private String[] valstrs;
		private ImsiTimeKey imsiKey;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			valstrs = value.toString().split(",", -1);
			if (valstrs.length < 6)
			{
				return;
			}
			imsi = valstrs[0];
			if (imsi.trim().length() < 5)
			{
				return;
			}
			imsiKey = new ImsiTimeKey(Long.parseLong(imsi), 0, 0, 0);
			context.write(imsiKey, value);
		}
	}

	public static class LocationMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String imsi = "";
		private int itime;
		private final int TimeSpan = 600;// 10分钟间隔
		private String[] valstrs;
		private ImsiTimeKey imsiKey;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			valstrs = value.toString().split("\\|" + "|" + "\t", -1);

			if (valstrs.length < 11)
			{
				return;
			}

			if (valstrs[0].equals("URI") || valstrs[0].equals(""))
			{
				imsi = valstrs[1];
			}
			else
			{
				imsi = valstrs[0];
			}

			if (imsi.trim().length() < 5 || imsi.equals("6061155539545534980"))
			{// 6061155539545534980是空字符加密后的结果
				return;
			}

			try
			{
				imsiKey = new ImsiTimeKey(Long.parseLong(imsi), 0, 0, 1);
				context.write(imsiKey, value);
			}
			catch (NumberFormatException e)
			{

			}
		}
	}

	public static class LableMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String beginTime = "";
		private String endTime = "";
		private String imsi = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private int etime = 0;
		private Date d_beginTime;
		private String[] valstrs;
		private ImsiTimeKey imsiKey;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private String xmString;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split("\t" + "|" + "\\|", -1);

			for (int i = 0; i < valstrs.length; i++)
			{
				valstrs[i] = valstrs[i].trim();
			}

			if (valstrs.length < 4)
			{
				return;
			}

			beginTime = valstrs[1];
			endTime = valstrs[2];
			imsi = valstrs[0].trim();

			if (beginTime.length() == 0 || imsi.trim().length() != 15)
			{
				return;
			}

			stime = 0;
			try
			{
				beginTime = beginTime.substring(0, beginTime.length() - 7);
				d_beginTime = format.parse(beginTime);
				stime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				return;
			}

			etime = 0;
			try
			{
				endTime = endTime.substring(0, endTime.length() - 7);
				d_beginTime = format.parse(endTime);
				etime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				return;
			}

			int tmSTime = stime / TimeSpan * TimeSpan;
			int tmETime = etime / TimeSpan * TimeSpan;
			if (tmSTime == tmETime)
			{
				imsiKey = new ImsiTimeKey(Long.parseLong(imsi), stime, stime / TimeSpan * TimeSpan, 2);
				context.write(imsiKey, value);
			}
			else
			{
				for (int tTime = tmSTime; tTime <= tmETime; tTime += TimeSpan)
				{
					imsiKey = new ImsiTimeKey(Long.parseLong(imsi), tTime, tTime, 2);
					context.write(imsiKey, value);
				}
			}
		}
	}

	public static class LocationWFMapper extends Mapper<Object, Text, ImsiTimeKey, Text>
	{
		private String imsi = "";
		private String[] valstrs;
		private ImsiTimeKey imsiKey;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			valstrs = value.toString().split("\\|" + "|" + "\t", 3);

			if (valstrs.length < 2)
			{
				return;
			}

			imsi = valstrs[0];

			/*
			 * if (imsi.length() != 15) { return; }
			 */

			imsiKey = new ImsiTimeKey(Long.parseLong(imsi), 0, 0, 3);
			context.write(imsiKey, value);
		}
	}

	public static class XdrDataMapper_MME extends DataDealMapper<Object, Text, ImsiTimeKey, Text>
	{
		private MultiOutputMng<ImsiTimeKey, Text> mosMng;

		private String beginTime = "";
		private String imsi = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private String xmString = "";
		private String[] valstrs;
		private long imsi_long;
		private long eci;
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;
		private Text curText = new Text();
		private String curStr = "";
		private List<String> colunNameList;

		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			// UEExclude.GetInstance().loadData();
			// load xdr prepare data
			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
			String imsiCountPath = conf.get("mastercom.mroxdrmerge.xdr.locfill.inpath_imsicount");
			loadErrUE(imsiCountPath);
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");

			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("Procedure_Start_Time,IMSI,Cell_ID");
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}

			String path_event_http = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_http");

			// 初始化输出控制
			if (path_event_http.contains(":"))
				mosMng = new MultiOutputMng<ImsiTimeKey, Text>(context, "");
			else
				mosMng = new MultiOutputMng<ImsiTimeKey, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.init();
			////////////////////

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.xdrFormat))// 开启xdrformat
			{
				colunNameList = new ArrayList<String>(parseItem.getColumPosMap().keySet());
				Collections.sort(colunNameList);// 对字段按照字段名称排序
			}
			// 按小区列表过滤
			FilterByEci.readEciList(conf);

		}

		private boolean loadErrUE(String imsiCountPath)
		{
			try
			{
				HDFSOper hdfsOper = null;
				if (!imsiCountPath.contains(":"))
				{
					hdfsOper = new HDFSOper(conf);
				}
				if (hdfsOper != null && hdfsOper.checkDirExist(imsiCountPath))
				{
					List<FileStatus> fileList = hdfsOper.listFileStatus(imsiCountPath, false);
					for (FileStatus fileInfo : fileList)
					{
						BufferedReader reader = null;
						try
						{
							reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(fileInfo.getPath()), "UTF-8"));
							String strData;
							String[] values;
							long imsi;
							while ((strData = reader.readLine()) != null)
							{
								if (strData.length() == 0)
								{
									continue;
								}
								values = strData.split("\t", -1);
								imsi = Long.parseLong(values[0]);
								UEExclude.GetInstance().addImsi(imsi);
							}
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "load imsi count error ", e);
						}
						finally
						{
							if (reader != null)
							{
								reader.close();
							}
						}
					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "load imsi count error : ", e);
				return false;
			}
			return true;
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		// 华为seq数据接入
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.xdrFormat))
			{
				valstrs = xmString.toString().split(parseItem.getSplitMark(), -1);
			}
			else
			{
				valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);
			}
			try
			{
				dataAdapterReader.readData(valstrs);
				d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
				eci = dataAdapterReader.GetLongValue("Cell_ID", 0);

				if (!FilterByEci.ifMap(eci))
				{
					return;
				}

				if (imsi_long <= 0 || imsi_long == 6061155539545534980L)
				{
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
					}
					return;
				}
				stime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "get data error : " + xmString, e);
				}
				return;
			}

			if (UEExclude.GetInstance().isUEExclude(imsi_long))
			{
				/*
				 * if (!MainModel.GetInstance().getCompile().Assert(
				 * CompileMark. NoCpeUser)) { imsiKey = new
				 * ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan,
				 * 11); curStr = DATATYPE_4G_MME + "#" + value.toString();
				 * curText.set(curStr); context.write(imsiKey, curText); }
				 */
			}
			else
			{
				imsiKey = new ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan, 10);
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.xdrFormat))// 开启xdrformat
				{
					String content = dataAdapterReader.getAppendString(colunNameList);
					curStr = DATATYPE_4G_MME + "#" + content;
				}
				else
				{
					curStr = DATATYPE_4G_MME + "#" + value.toString();
				}
				curText.set(curStr);
				context.write(imsiKey, curText);
			}
		}
	}

	public static class XdrDataMapper_HTTP extends DataDealMapper<Object, Text, ImsiTimeKey, Text>
	{
		private MultiOutputMng<ImsiTimeKey, Text> mosMng;

		private String beginTime = "";
		private final int TimeSpan = 600;// 10分钟间隔
		private int stime = 0;
		private Date d_beginTime;
		private ImsiTimeKey imsiKey;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private String xmString = "";
		private String[] valstrs;
		private long imsi_long;
		private long eci;
		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;
		private Text curText = new Text();
		private String curStr = "";

		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			MainModel.GetInstance().setConf(conf);

			String imsiCountPath = conf.get("mastercom.mroxdrmerge.xdr.locfill.inpath_imsicount");
			loadErrUE(imsiCountPath);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("Procedure_Start_Time,IMSI,longitude,Cell_ID");
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}

			String path_event_http = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_http");

			// 初始化输出控制
			if (path_event_http.contains(":"))
				mosMng = new MultiOutputMng<ImsiTimeKey, Text>(context, "");
			else
				mosMng = new MultiOutputMng<ImsiTimeKey, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.init();
			////////////////////
			// 按小区列表过滤
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.FilterCell))
			{
				if (!FilterCellConfig.GetInstance().loadFilterCell(conf))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "filtercell init error 请检查！" + CellConfig.GetInstance().errLog);
					throw (new IOException("filtercell init error 请检查！"));
				}
			}
		}

		private boolean loadErrUE(String imsiCountPath)
		{
			if (imsiCountPath.equals("NULL") || imsiCountPath.contains(":"))
				return false;
			try
			{
				HDFSOper hdfsOper = new HDFSOper(conf);
				if (hdfsOper.checkDirExist(imsiCountPath))
				{
					List<FileStatus> fileList = hdfsOper.listFileStatus(imsiCountPath, false);
					for (FileStatus fileInfo : fileList)
					{
						BufferedReader reader = null;
						try
						{
							reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(fileInfo.getPath()), "UTF-8"));
							String strData;
							String[] values;
							long imsi;
							while ((strData = reader.readLine()) != null)
							{
								if (strData.length() == 0)
								{
									continue;
								}

								values = strData.split("\t", -1);
								imsi = Long.parseLong(values[0]);
								UEExclude.GetInstance().addImsi(imsi);
							}
						}
						catch (Exception e)
						{
							if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
							{
								LOGHelper.GetLogger().writeLog(LogType.error, "load imsi count error ", e);
							}
						}
						finally
						{
							if (reader != null)
							{
								reader.close();
							}
						}
					}

				}
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "load imsi count error : ", e);
				}
				return false;
			}
			return true;
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		// 华为seq数据接入
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

			try
			{
				dataAdapterReader.readData(valstrs);
				d_beginTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				imsi_long = dataAdapterReader.GetLongValue("IMSI", 0);
				eci = dataAdapterReader.GetLongValue("Cell_ID", 0);
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.FilterCell))
				{
					if (!FilterCellConfig.GetInstance().getLteCell(eci))
					{
						return;
					}
				}
				double longitude = dataAdapterReader.GetDoubleValue("longitude", 0);
				if (longitude <= 0)
				{
					return;
				}
				if (imsi_long <= 0 || imsi_long == 6061155539545534980L)
				{
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "get data error :" + xmString);
					}
					return;
				}
				stime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "get data error : " + xmString, e);
				}
				return;
			}

			if (UEExclude.GetInstance().isUEExclude(imsi_long))
			{
				// 不要cpe用户
				/*
				 * if (!MainModel.GetInstance().getCompile().Assert(CompileMark.
				 * NoCpeUser)) { imsiKey = new ImsiTimeKey(imsi_long, stime,
				 * stime / TimeSpan * TimeSpan, 11); curStr = DATATYPE_4G_HTTP +
				 * "#" + value.toString(); curText.set(curStr);
				 * context.write(imsiKey, curText); }
				 */
			}
			else
			{
				imsiKey = new ImsiTimeKey(imsi_long, stime, stime / TimeSpan * TimeSpan, 10);
				curStr = DATATYPE_4G_HTTP + "#" + value.toString();
				curText.set(curStr);
				context.write(imsiKey, curText);
			}
		}
	}

	public static class XdrDataMapper_23G extends Mapper<Object, Text, ImsiTimeKey, Text>
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
		private Text curText = new Text();
		private String curStr = "";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split("\t", -1);

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

			imsiKey = new ImsiTimeKey(Long.parseLong(imsi), stime, stime / TimeSpan * TimeSpan, 10);
			curStr = DATATYPE_23G + "#" + value.toString();
			curText.set(curStr);
			context.write(imsiKey, curText);
		}

	}

	public static class ImsiPartitioner extends Partitioner<ImsiTimeKey, Text> implements Configurable
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
			// return Math.abs((int)(key.getImsi() & 0x00000000ffffffffL)) %
			// numOfReducer;

			if (key.getDataType() == 4)// cpe用户
			{
				return Math.abs((key.getImsi() + "_" + key.getTime() / 3600 * 3600).hashCode()) % numOfReducer;
			}
			else
			{
				return Math.abs(("" + key.getImsi()).hashCode()) % numOfReducer;
			}
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
				if (s1.getTimeSpan() > s2.getTimeSpan())
				{
					return 1;
				}
				else if (s1.getTimeSpan() < s2.getTimeSpan())
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
