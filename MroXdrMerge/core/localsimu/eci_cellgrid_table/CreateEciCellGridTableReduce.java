package localsimu.eci_cellgrid_table;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mroxdrmerge.MainModel;

public class CreateEciCellGridTableReduce
{
	public static class CreateEciCellGridTableReducer extends DataDealReducer<GridKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private String EciTablePath;
		private GridKey proEciTableKey = null;
		private String eciArray = "";
		private Text curText = new Text();
		protected static final Log LOG = LogFactory.getLog(CreateEciCellGridTableReducer.class);
		private FileSystem fs = null;
		private HashMap<String, FSDataOutputStream> ioMap = new HashMap<String, FSDataOutputStream>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			EciTablePath = conf.get("mastercom.cellgrid.enbidtable.EciTablePath");
			this.context = context;
			// 初始化输出控制
			// mosMng = new MultiOutputMng<NullWritable, Text>(context,
			// MainModel.GetInstance().getFsUrl());
			// String eciConfigTable =
			// MainModel.GetInstance().getAppConfig().getEciConfigPath();
			// EciTableConfig.GetInstance().loadEnbidTable(conf,
			// eciConfigTable);
			// HashMap<Long, Integer> eciMap =
			// EciTableConfig.GetInstance().getEciConfigTableMap();
			// 注册输出表名
			// for (long eci : eciMap.keySet())
			// {
			// mosMng.SetOutputPath(eci + "", EciTablePath);
			// }
			// mosMng.init();
			// 得到fs

			try
			{
				//xsh
				HDFSOper hdfsOper = new HDFSOper(conf);
//				HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//						MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
				fs = hdfsOper.getFs();
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			// mosMng.close();
			for (String eci : ioMap.keySet())
			{
				ioMap.get(eci).close();
			}
			ioMap.clear();
		}

		@Override
		protected void reduce(GridKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			if (key.getDataType() == 1)// 记录下要写cellgrid的eci索引
			{
				proEciTableKey = key;
				for (Text eciList : values)// 应该只有一条记录
				{
					eciArray = eciList.toString();
				}
			}
			else if (key.getDataType() == 2 && proEciTableKey != null)
			{
				if (key.getLongitude() == proEciTableKey.getLongitude()
						&& key.getLatitude() == proEciTableKey.getLatitude() && eciArray.length() > 0)
				{
					StringBuffer bf = new StringBuffer();
					for (Text cellGrid : values)
					{
						bf.append(cellGrid.toString() + "\r\n");
					}
					curText.set(bf.toString());
					String[] tempEcis = eciArray.split(",|\t", -1);
					for (String eci : tempEcis)
					{
						// mosMng.write(eci, NullWritable.get(), curText);
						writeCellGrid(eci, bf.toString(), ioMap);
					}
				}
			}
		}

		public void writeCellGrid(String eci, String content, HashMap<String, FSDataOutputStream> ioMap)
				throws IllegalArgumentException, IOException
		{
			Path path = new Path(EciTablePath);
			FSDataOutputStream out = null;
			if (!fs.exists(path))
			{
				fs.mkdirs(path);
			}
			Path filePath = new Path(EciTablePath + "/" + eci + ".txt");
			if (ioMap.containsKey(eci))
			{
				out = ioMap.get(eci);
			}
			else
			{
				out = fs.create(filePath, false);
				ioMap.put(eci, out);
			}
			out.writeChars(content);
			out.flush();
		}
	}
}
