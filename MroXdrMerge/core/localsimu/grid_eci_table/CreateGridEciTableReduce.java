package localsimu.grid_eci_table;

import java.io.IOException;
import java.util.HashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;

public class CreateGridEciTableReduce
{
	public static class CreateGridEciTableReducer extends DataDealReducer<GridKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private String gridEciTablePath;
		protected static final Log LOG = LogFactory.getLog(CreateGridEciTableReducer.class);

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			gridEciTablePath = conf.get("mastercom.cellgrid.enbidtable.gridEciTablePath");
			this.context = context;
			// 初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.SetOutputPath("gridEciTable", gridEciTablePath);
			mosMng.init();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			mosMng.close();
		}

		@Override
		protected void reduce(GridKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			HashSet<Integer> set = new HashSet<Integer>();
			String enbids = "";
			for (Text value : values)
			{
				set.add(Integer.parseInt(value.toString()));
			}
			for (int enbid : set)
			{
				enbids = enbids + enbid + ",";
			}
			curText.set(key.getLongitude() + "_" + key.getLatitude() + ":" + enbids.substring(0, enbids.length() - 1));
			mosMng.write("gridEciTable", NullWritable.get(), curText);
		}
	}
}
