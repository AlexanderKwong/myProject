package xdr.locallex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import jan.com.hadoop.mapred.CombineSmallFileRecordReader;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import jan.util.StringHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.locallex.model.XdrDataBase;
import xdr.locallex.model.XdrDataFactory;

public class LocAllMapper
{
	public static class UserLocMapper_XDRLOC extends DataDealMapper<Object, Text, ImsiKey, Text>
	{
		private String locString = "";
		private String[] valstrs;
		private long imsi;
		private int ilongtitude;
		private int ilatitude;
		private ImsiKey imsiKey;
		private ImsiKey s1apidKey;
		private Text curText = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			locString = value.toString();
			valstrs = locString.split("\t");
			try
			{
				imsi = Long.parseLong(valstrs[3]);
				ilongtitude = Integer.parseInt(valstrs[4]);
				ilatitude = Integer.parseInt(valstrs[5]);

				if (ilongtitude <= 0 || ilatitude <= 0)
				{
					return;
				}

				imsiKey = new ImsiKey(imsi, XdrDataFactory.LOCTYPE_XDRLOC, 0, 0);
				context.write(imsiKey, value);
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)
						|| MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
				{
					long s1apid = Long.parseLong(valstrs[21]);
					long eci = Long.parseLong(valstrs[18]);
					s1apidKey = new ImsiKey(0, XdrDataFactory.LOCTYPE_XDRLOC, s1apid, eci);
					context.write(s1apidKey, value);
				}
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + locString, e);
				}
			}
		}
	}

	public static class UserLocMapper_MRLOC extends DataDealMapper<Object, Text, ImsiKey, Text>
	{
		private String locString = "";
		private String[] valstrs;
		private long imsi;
		private ImsiKey imsiKey;
		private Text curText = new Text();
		private ImsiKey s1apidKey;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			locString = value.toString();
			valstrs = locString.split("\t");
			try
			{
				imsi = Long.parseLong(valstrs[3]);
				imsiKey = new ImsiKey(imsi, XdrDataFactory.LOCTYPE_MRLOC, 0, 0);
				context.write(imsiKey, value);
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)
					|| MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
				{
					// 位置库的eci不能错
					long eci = Long.parseLong(valstrs[18]);
					long s1apid = Long.parseLong(valstrs[21]);
					s1apidKey = new ImsiKey(0, XdrDataFactory.LOCTYPE_XDRLOC, s1apid, eci);
					context.write(s1apidKey, value);
				}
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					e.printStackTrace();
					LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + locString, e);
				}
			}
		}
	}

	public static class XdrDataMapper extends DataDealMapper<Object, Text, ImsiKey, Text>
	{
		private int dataType = 0;
		private Long imsi = 0l;
		private long s1apid = 0L;
		private long ecgi = 0L;

		private XdrDataBase xdrDataItem = null;
		private String curFileSplitPath = "";
		private String tmpInPath = "";
		private Map<String, Integer> pathIndexMap = null;

		private ParseItem curParseItem = null;
		private DataAdapterReader curDataAdapterReader = null;
		private int splitMax = -1;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			initData(context);
		}

		private void initData(Context context)
		{
			// pathIndex: 1,/mt_wlyh/Data/test1$2,/mt_wlyh/Data/test2
			String inpathindex = conf.get("mastercom.mroxdrmerge.locall.inpathindex");
			pathIndexMap = new HashMap<String, Integer>();
			for (String pathPare : inpathindex.split("\\$"))
			{
				if (pathPare.length() == 0)
				{
					continue;
				}

				String[] ppPare = pathPare.split(";");
				String ppPath = ppPare[1];

				for (String pp : ppPath.split(","))
				{
					String tm_formatPath = pp;
					if (pp.indexOf("hdfs://") >= 0)
					{
						int tm_sPos = tm_formatPath.indexOf("/", ("hdfs://").length());
						tm_formatPath = tm_formatPath.substring(tm_sPos);
					}

					tm_formatPath = StringHelper.SideTrim(tm_formatPath, "/");
					tm_formatPath = StringHelper.SideTrim(tm_formatPath, "\\\\");
					tm_formatPath = "/" + tm_formatPath;

					pathIndexMap.put(tm_formatPath, Integer.parseInt(ppPare[0]));
				}
			}
		}

		private XdrDataBase initDataType(String dirPath) throws InterruptedException
		{
			// hdfs://node001:9000/mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_160421/TB_SIGNAL_CELL_01_160421
			int sPos = dirPath.indexOf("/", ("hdfs://").length());
			String formatPath = dirPath.substring(sPos);
			if (dirPath.startsWith("file:"))
			{
				formatPath = dirPath.replace("file:", "");
			}
			if (!pathIndexMap.containsKey(formatPath))
			{
				throw new InterruptedException("path index is not found, please check : " + formatPath);
			}

			int mergeType = pathIndexMap.get(formatPath);

			try
			{
				return XdrDataFactory.GetInstance().getXdrDataObject(mergeType);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "mergeType: " + mergeType + " formatPath: " + formatPath);
				throw new InterruptedException("init data type error :" + mergeType + " " + e.getMessage());
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
			// get xdrdatabase to deal data
			tmpInPath = context.getConfiguration().get(CombineSmallFileRecordReader.CombineSmallFilePath);
			if (curFileSplitPath.length() != tmpInPath.length() || !curFileSplitPath.equals(tmpInPath))
			{
				xdrDataItem = initDataType(tmpInPath);
				curFileSplitPath = tmpInPath;

				curParseItem = xdrDataItem.getDataParseItem();
				if (curParseItem == null)
				{
					throw new IOException("parse item do not get.");
				}
				curDataAdapterReader = new DataAdapterReader(curParseItem);
			}

			String[] valstrs = value.toString().split(curParseItem.getSplitMark(), -1);
			curDataAdapterReader.readData(valstrs);
			try
			{
				if (!xdrDataItem.FillData_short(curDataAdapterReader))
				{
					return;
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "mergeDataDo.fillData error : " + value.toString(), e);
				return;
			}

			dataType = xdrDataItem.getDataType();
			imsi = xdrDataItem.getImsi();
			s1apid = xdrDataItem.getS1apid();
			ecgi = xdrDataItem.getEcgi();
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi))
			{
				if (imsi <= 0)
				{
					return;
				}
			}
			ImsiKey itemKey = new ImsiKey(imsi, dataType, s1apid, ecgi);

			context.write(itemKey, value);
		}

	}

	public static class ImsiPartitioner extends Partitioner<ImsiKey, Text> implements Configurable
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
			if (key.getImsi() > 0)
			{
				return Math.abs(("" + key.getImsi()).hashCode()) % numOfReducer;
			}
			else
			{
				return Math.abs(("" + key.getS1apid() + key.getEci()).hashCode()) % numOfReducer;
			}
		}

	}

	public static class ImsiSortKeyComparator extends WritableComparator
	{
		public ImsiSortKeyComparator()
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

	public static class ImsiSortKeyGroupComparator extends WritableComparator
	{

		public ImsiSortKeyGroupComparator()
		{
			super(ImsiKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{

			ImsiKey s1 = (ImsiKey) a;
			ImsiKey s2 = (ImsiKey) b;
			if (s1.getImsi() > 0 && s2.getImsi() > 0)
			{
				return compareImsi(a, b);
			}
			else if (s1.getImsi() <= 0 && s2.getImsi() <= 0)
			{
				return compareS1apidAndEci(a, b);
			}
			else
			{
				return compareImsi(a, b);
			}

		}

		private int compareImsi(Object a, Object b)
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

		private int compareS1apidAndEci(Object a, Object b)
		{
			ImsiKey s1 = (ImsiKey) a;
			ImsiKey s2 = (ImsiKey) b;

			if (s1.getS1apid() > s2.getS1apid())
			{
				return 1;
			}
			else if (s1.getS1apid() < s2.getS1apid())
			{
				return -1;
			}
			else
			{
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
