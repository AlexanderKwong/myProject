
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import StructData.DT_Sample_4G;
import StructData.GridItem;
import jan.com.hadoop.mapred.DataDealConfiguration;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import mro.lablefill.StatDeal;
import mroxdrmerge.MainModel;
import util.DataGeter;
import xdr.lablefill.HourDataDeal_4G;
import xdr.lablefill.HourDataDeal_4G.HourDataItem;
import xdr.lablefill.ResultHelper;
import xdr.lablefill.UserGridStat_4G;

public class MroSampleSimu
{

	public static class MyMapper extends DataDealMapper<Object, Text, Text, Text>
	{
		private Text valueText = new Text();
		private Text keyText = new Text();
		private String strutf;
		private String str_time;
		private String str_imsi;
		private String str_long;
		private String str_lat;
		private StringBuffer sb = new StringBuffer();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			strutf = new String(value.toString().getBytes(), "UTF-8");
			
			String[] valstrs = strutf.toString().split("\t", -1);	
			if(valstrs.length < 163)
			{
				return;
			}

		    str_time = valstrs[2];
			str_imsi = valstrs[12];
			str_long = valstrs[161];
			str_lat = valstrs[162];
			
			if (str_time.length() == 0 || str_imsi.length() != 15 || str_long.length() <= 4 || str_lat.length() <= 4)
			{
				return;
			}
			
			keyText.set(str_imsi);
			
			sb.delete(0, sb.length());
			sb.append(str_time);sb.append("_");
			sb.append(str_imsi);sb.append("_");
			sb.append(str_long);sb.append("_");
			sb.append(str_lat);
			
			valueText.set(sb.toString());

			context.write(keyText, valueText);
		}
			
	}
	
	
	public static class MyReducer extends DataDealReducer<Text, Text, NullWritable, Text>
	{
		private Text curText = new Text();
		private MultiOutputMng<NullWritable, Text> mosMng;
		private String xdrgriduserhour;
		private String str_time = "";
		private String str_imsi = "";
		private String str_long = "";
		private String str_lat = "";
		protected HourDataDeal_4G hourDataDeal_4G;

		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			Configuration conf = context.getConfiguration();
			xdrgriduserhour = conf.get("mastercom.mroxdrmerge.MroSampleSimu.xdrgriduserhour");
				
			//初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());	
			mosMng.SetOutputPath("xdrgriduserhour", xdrgriduserhour);	
			
			mosMng.init();
			////////////////////
			
			hourDataDeal_4G = new HourDataDeal_4G(StatDeal.STATDEAL_ALL);
		}
		
		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for (HourDataItem hourData : hourDataDeal_4G.getHourDataDealMap().values())
			{
				for (Map.Entry<GridItem, UserGridStat_4G> valuePare : hourData.getUserGirdDataMap().entrySet())
				{
					valuePare.getValue().finalDeal();
					try
					{
						curText.set(ResultHelper.getPutUserGridInfo(valuePare.getValue().getUserGrid()));
						mosMng.write("xdrgriduserhour", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}
				
			}
			
			
			super.cleanup(context);
			
			mosMng.close();
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] vals;
			DT_Sample_4G sample = new DT_Sample_4G();
			
			for (Text text : values)
			{
				sample.Clear();
				
				vals = text.toString().split("_", -1);
			
				str_time = vals[0];
                str_imsi  = vals[1];	
                str_long = vals[2];
                str_lat = vals[3];
                
                sample.itime = DataGeter.GetInt(str_time);
                sample.IMSI = DataGeter.GetLong(str_imsi);
                
                str_long = str_long.substring(0, str_long.length()-3);    
                str_lat = str_lat.substring(0, str_lat.length()-3);   
                sample.ilongitude = (int)(DataGeter.GetDouble(str_long) * 1000000000);
                sample.ilatitude = (int)(DataGeter.GetDouble(str_lat) * 100000000);
                  
                hourDataDeal_4G.dealXdr(sample);
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
		String outpath_table = args[1];
		
		String outpath = outpath_table + "/output";
		String xdrgriduserhour = outpath_table + "/USER_GRID";

		conf.set("mastercom.mroxdrmerge.MroSampleSimu.inpath_xdr", inpath_xdr);
		conf.set("mastercom.mroxdrmerge.MroSampleSimu.outpath_table", outpath_table);
		conf.set("mastercom.mroxdrmerge.MroSampleSimu.xdrgriduserhour", xdrgriduserhour);

		conf.set("mapreduce.job.queuename", "network");
		
		
	    //hadoop system set 
	    conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");//default 0.05
	    conf.set("mapreduce.task.io.sort.mb", "1024");
	    conf.set("mapreduce.map.memory.mb", "3072");
	    conf.set("mapreduce.reduce.memory.mb", "8192");
	    conf.set("mapreduce.map.java.opts", "-Xmx2048M");
	    conf.set("mapreduce.reduce.java.opts", "-Xmx6140M");
	    
	    conf.set("mapreduce.reduce.speculative", "false");//停止推测功能
	      
	    //将小文件进行整合
		long splitMinSize = 512 * 1024 * 1024;
	    conf.set("mapreduce.input.fileinputformat.split.maxsize",String.valueOf(splitMinSize));   
		long minsizePerNode = 10 * 1024 * 1024;
	    conf.set("mapreduce.input.fileinputformat.split.minsize.per.node",String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
	    conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack",String.valueOf(minsizePerRack));
	    
	    //初始化自己的配置管理
	    DataDealConfiguration.create(outpath_table, conf);
		
		
		Job job = Job.getInstance(conf, "MroSampleSimu");

		job.setJarByClass(MroSampleSimu.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(200);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(inpath_xdr));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		MultipleOutputs.addNamedOutput(job, "xdrgriduserhour", TextOutputFormat.class, NullWritable.class, Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
