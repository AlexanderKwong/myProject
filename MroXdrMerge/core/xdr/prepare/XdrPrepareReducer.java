package xdr.prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;

public class XdrPrepareReducer
{

	public static class StatReducer extends DataDealReducer<Text, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private String[] valStr;
		
		private String path_ImsiCount;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
			path_ImsiCount = conf.get("mastercom.mroxdrmerge.xdrprepare.path_ImsiCount");
				
			//初始化输出控制
			//mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getAppConfig().getFsUri());
			if(path_ImsiCount.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
				
			mosMng.SetOutputPath("imsicount", path_ImsiCount);	
			
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

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			
            int total = 0;
            boolean isCPE = false;
			for (Text value : values)
			{
			    valStr = value.toString().split("\t", -1);
			    if(valStr.length == 3)
			    {
			    	total += Integer.parseInt(valStr[1]);
			    	if(Integer.parseInt(valStr[2]) > 0)
			    	{
			    		isCPE = true;
			    	}
			    }	
			}
			
			if(total > 300000)
			{
				curText.set(key.toString() + "\t" + total);
				mosMng.write("imsicount", NullWritable.get(), curText);
			}
		}

	}

}
