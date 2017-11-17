package mergestat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.StringHelper;
import mroxdrmerge.MainModel;

public class MergeStatReducer
{

	public static class StatReducer extends DataDealReducer<MergeKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();

		private String[] strs;
		private Map<Integer, String> outpathIndexMap;
		private Map<Integer, String> outtypeIndexMap;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);

			// outPathIndex: 1;stat1;/mt_wlyh/test1/$2;stat2;/mt_wlyh/test2/
			String outPathIndex = conf.get("mastercom.mroxdrmerge.mergestat.outpathindex");
			outpathIndexMap = new HashMap<Integer, String>();
			outtypeIndexMap = new HashMap<Integer, String>();
			for (String pathPare : outPathIndex.split("\\$"))
			{
				String[] ppPare = pathPare.split(";");

				String ppType = ppPare[1];
				ppType = StringHelper.SideTrim(ppType, " ");

				String ppPath = ppPare[2];
				if (ppPath.indexOf("hdfs://") >= 0)
				{
					int tm_sPos = ppPath.indexOf("/", ("hdfs://").length());
					ppPath = ppPath.substring(tm_sPos);
				}
				ppPath = StringHelper.SideTrim(ppPath, " ");
				ppPath = StringHelper.SideTrim(ppPath, "/");
				ppPath = StringHelper.SideTrim(ppPath, "\\\\");
				if (ppPath.indexOf("hdfs://") < 0)
				{
					ppPath = "/" + ppPath;
				}

				outpathIndexMap.put(Integer.parseInt(ppPare[0]), ppPath);
				outtypeIndexMap.put(Integer.parseInt(ppPare[0]), ppType);
			}

			// 初始化输出控制
			if (outPathIndex.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			for (int index : outpathIndexMap.keySet())
			{
				mosMng.SetOutputPath(outtypeIndexMap.get(index), outpathIndexMap.get(index));
			}

			mosMng.init();

			////////////////////

		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);

			mosMng.close();
		}

		public void reduce(MergeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			IMergeDataDo mergeResult = null;
			IMergeDataDo dataTemp = null;
			try
			{
				dataTemp = MergeDataFactory.GetInstance().getMergeDataObject(key.getDataType());
			}
			catch (Exception e)
			{
				throw new InterruptedException("init data type error ");
			}

			for (Text value : values)
			{
				strs = value.toString().split("\t", -1);

				dataTemp.fillData(strs, 0);

				if (mergeResult == null)
				{
					mergeResult = dataTemp;
					try
					{
						dataTemp = MergeDataFactory.GetInstance().getMergeDataObject(key.getDataType());
					}
					catch (Exception e)
					{
						throw new InterruptedException("init data type error ");
					}
				}

				mergeResult.mergeData(dataTemp);
			}

			curText.set(mergeResult.getData());
			mosMng.write(outtypeIndexMap.get(key.getDataType()), NullWritable.get(), curText);

		}

	}

}
