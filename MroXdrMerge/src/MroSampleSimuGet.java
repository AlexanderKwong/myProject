
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealConfiguration;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;
import util.DataGeter;

public class MroSampleSimuGet
{
	
	public static class XdrMapper extends DataDealMapper<Object, Text, NullWritable, Text>
	{
		private String xmString = "";
		private String[] valstrs;
		private int cityid = 0;
		private MultiOutputMng<NullWritable, Text> mosMng;
		private String dataPath = "";
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			Configuration conf = context.getConfiguration();
			dataPath = conf.get("mastercom.mroxdrmerge.dataPath");

			//初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());	
			mosMng.SetOutputPath("mrosample", dataPath);
			
			mosMng.init();
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

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
			valstrs = xmString.toString().split("\t", 3);
					
			try
			{	
				cityid = DataGeter.GetInt(valstrs[0]);
				if(cityid == 3)
				{
					mosMng.write("mrosample", NullWritable.get(), value);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + xmString, e);
			}
			
		}
		
	}

	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		if (args.length != 2)
		{
			System.err.println("Usage: MroXdrMerger TestData <in-data> <in-data> <hour time> ");
			throw (new Exception("MroXdrMerger args input error!"));
		}

		String inpath_xdr = args[0];
		String outpath = args[1];
		String outpath_sample = outpath + "/" + "sample";
		String outpath_out = outpath + "/" + "out";
		
		conf.set("mapreduce.job.queuename", "plan");
		conf.set("mastercom.mroxdrmerge.dataPath", outpath_sample);
		
	    //初始化自己的配置管理
	    DataDealConfiguration.create(outpath, conf);
	    

		Job job = Job.getInstance(conf, "MroSampleSimuGet");
		
        job.setJarByClass(MroSampleSimuGet.class);  
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(XdrMapper.class);   
        job.setMapOutputKeyClass(NullWritable.class);  
        job.setMapOutputValueClass(Text.class); 
        
        job.setNumReduceTasks(0);	
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);   
        
        MultipleOutputs.addNamedOutput(job, "mrosample", TextOutputFormat.class, NullWritable.class, Text.class);
        
		FileInputFormat.setInputPaths(job, new Path(inpath_xdr));
		FileOutputFormat.setOutputPath(job, new Path(outpath_out));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
