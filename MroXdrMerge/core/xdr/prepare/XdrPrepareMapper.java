package xdr.prepare;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.chinamobile.xdr.LocationInfo;
import com.chinamobile.xdr.demo;

import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class XdrPrepareMapper
{
	public static class XdrDataMapper_MME extends DataDealMapper<Object, Text, Text, Text>
	{
		private MultiOutputMng<Text, Text> mosMng;

		private String xmString = "";
		private StringBuffer sb = new StringBuffer();
		private String[] valstrs;
		private long imsi;
		private Map<Long, CPEUserItem> cpeMap = new HashMap<Long, CPEUserItem>();
		private Text curText = new Text();
		private Text curTextKey = new Text();

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("IMSI");
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}

			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
			String path_Location = conf.get("mastercom.mroxdrmerge.xdrprepare.path_Location");

			// 初始化输出控制
			// mosMng = new MultiOutputMng<Text, Text>(context,
			// MainModel.GetInstance().getAppConfig().getFsUri());
			if (path_Location.contains(":"))
				mosMng = new MultiOutputMng<Text, Text>(context, "");
			else
				mosMng = new MultiOutputMng<Text, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.init();
			////////////////////
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for (Map.Entry<Long, CPEUserItem> imsiMapEntry : cpeMap.entrySet())
			{
				sb.delete(0, sb.length());
				sb.append(imsiMapEntry.getValue().imsi);
				sb.append("\t");
				sb.append(imsiMapEntry.getValue().count);
				sb.append("\t");
				sb.append(imsiMapEntry.getValue().isCPE);

				curTextKey.set(imsiMapEntry.getKey().toString());
				curText.set(sb.toString());
				context.write(curTextKey, curText);
			}

			super.cleanup(context);

		}

		// 华为seq数据
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

			try
			{
				dataAdapterReader.readData(valstrs);
				imsi = dataAdapterReader.GetLongValue("IMSI", -1);

				if (imsi <= 0)
				{
					return;
				}

				CPEUserItem cpeItem = cpeMap.get(imsi);
				if (cpeItem == null)
				{
					cpeItem = new CPEUserItem();
					cpeItem.imsi = imsi;
					cpeItem.count = 0;
					cpeItem.isCPE = 0;
					cpeMap.put(imsi, cpeItem);
				}

				cpeItem.count++;
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + xmString, e);
				}
			}

		}
	}

	public static class XdrDataMapper_HTTP extends DataDealMapper<Object, Text, Text, Text>
	{
		private MultiOutputMng<Text, Text> mosMng;

		private String xmString = "";
		private StringBuffer sb = new StringBuffer();
		private String[] valstrs;
		private long imsi;
		private String userIP;
		private int userPort;
		private String serverIP;
		private String host;
		private String timeStr;
		private String uri;
		private String HTTP_content_type = "";
		private String http_post_content = "";
		private int eci;
		private Date dateTime;
		private Map<Long, CPEUserItem> cpeMap = new HashMap<Long, CPEUserItem>();
		private Text curText = new Text();
		private Text curTextKey = new Text();
		private String requestType = "POST";

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		private int splitMax = -1;
		private String path_ImsiIP;
		private String path_Location;

		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private DecimalFormat df = new DecimalFormat(".#######");

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("IMSI,User_IP_IPv4,User_Port,App_Server_IP_IPv4,HOST,Procedure_Start_Time,URI,HTTP_content_type,Cell_ID");
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}

			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
			path_ImsiIP = conf.get("mastercom.mroxdrmerge.xdrprepare.path_ImsiIP");
			path_Location = conf.get("mastercom.mroxdrmerge.xdrprepare.path_Location");

			// 初始化输出控制
//			 mosMng = new MultiOutputMng<Test, Test>(context,
			// MainModel.GetInstance().getAppConfig().getFsUri());
			if (path_Location.contains(":"))
				mosMng = new MultiOutputMng<Text, Text>(context, "");
			else
				mosMng = new MultiOutputMng<Text, Text>(context, MainModel.GetInstance().getFsUrl());

			mosMng.SetOutputPath("imsiip", path_ImsiIP);
			mosMng.SetOutputPath("location", path_Location);

			mosMng.init();
			////////////////////

		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for (Map.Entry<Long, CPEUserItem> imsiMapEntry : cpeMap.entrySet())
			{
				sb.delete(0, sb.length());
				sb.append(imsiMapEntry.getValue().imsi);
				sb.append("\t");
				sb.append(imsiMapEntry.getValue().count);
				sb.append("\t");
				sb.append(imsiMapEntry.getValue().isCPE);

				curTextKey.set(imsiMapEntry.getKey().toString());
				curText.set(sb.toString());
				context.write(curTextKey, curText);
			}

			super.cleanup(context);
			mosMng.close();
		}

		// 华为seq数据
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

			// LOGHelper.GetLogger().writeLog(LogType.error,xmString);

			try
			{
				dataAdapterReader.readData(valstrs);
				imsi = dataAdapterReader.GetLongValue("IMSI", -1);
				userIP = dataAdapterReader.GetStrValue("User_IP_IPv4", "");
				serverIP = dataAdapterReader.GetStrValue("App_Server_IP_IPv4", "");
				host = dataAdapterReader.GetStrValue("HOST", "");
				dateTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				uri = dataAdapterReader.GetStrValue("URI", "");
				HTTP_content_type = dataAdapterReader.GetStrValue("HTTP_content_type", "");
				try
				{
					eci = dataAdapterReader.GetIntValue("Cell_ID", -1);
					userPort = dataAdapterReader.GetIntValue("User_Port", -1);
				}
				catch (Exception e1)
				{
				}
				if (imsi <= 0)
				{
					return;
				}

				CPEUserItem cpeItem = cpeMap.get(imsi);
				if (cpeItem == null)
				{
					cpeItem = new CPEUserItem();
					cpeItem.imsi = imsi;
					cpeItem.count = 0;
					cpeItem.isCPE = 0;
					cpeMap.put(imsi, cpeItem);
				}

				cpeItem.count++;
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi))
				{
					if (HostMng.GetInstance().isXdrLocation(host))
					{
						try
						{
							curTextKey.set("");

							sb.delete(0, sb.length());
							sb.append(imsi);
							sb.append("\t");
							sb.append(userIP);
							sb.append("\t");
							sb.append(userPort);
							sb.append("\t");
							sb.append(serverIP);
							sb.append("\t");
							sb.append(dateTime.getTime() / 1000L);
							curText.set(sb.toString());
							mosMng.write("imsiip", curTextKey, curText);
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + xmString, e);
						}
					}
				}

				if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
				{
					List<LocationInfo> filledLocationInfoList = demo.DecryptLoc(requestType, host, uri, HTTP_content_type, http_post_content, false);

					for (LocationInfo locationInfo : filledLocationInfoList)
					{
						// if (HostMng.GetInstance().isXdrLocation(host))
						{
							curTextKey.set("URI");

							sb.delete(0, sb.length());
							sb.append(imsi);
							sb.append("|");
							sb.append(dateTime.getTime());
							sb.append("|");
							sb.append(dateTime.getTime());
							sb.append("|");
							sb.append(eci);
							sb.append("|");
							sb.append(userIP);
							sb.append("|");
							sb.append(userPort);
							sb.append("|");
							sb.append(serverIP);
							sb.append("|");
							sb.append(demo.GetLocation(host, locationInfo.locationType));
							sb.append("|");
							sb.append(demo.GetLoctp(locationInfo.locationType));
							sb.append("|");
							sb.append(locationInfo.radius);
							sb.append("|");
							sb.append(df.format(locationInfo.longitude));
							sb.append("|");
							sb.append(df.format(locationInfo.latitude));
							sb.append("|");
							sb.append(uri.length());
							sb.append("|");
							sb.append(host);

							curText.set(sb.toString());
							mosMng.write("location", curTextKey, curText);

						}
					}
				}
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + xmString, e);
				}
			}
		}
	}
}
