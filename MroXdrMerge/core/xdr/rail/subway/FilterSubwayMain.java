package xdr.rail.subway;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mroxdrmerge.MainModel;
import xdr.rail.subway.FilterSubwayMapper.mmeMaper;
import xdr.rail.subway.FilterSubwayReduce.mmeReduce;

public class FilterSubwayMain
{
	private static String inputPath;
	private static String outputPath;
	public static void main(String[] args)
	{

		try{
			inputPath = args[0];
			outputPath = args[1]; 
			
			Configuration conf = new Configuration();
//			conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
			
			conf.set("inputPath", inputPath);
			conf.set("outputPath", outputPath);
			
			MainModel.GetInstance().setConf(conf);
			// 将小文件进行整合
			long splitMinSize = 128 * 1024 * 1024;
			conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
			long minsizePerNode = 10 * 1024 * 1024;
			conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
			long minsizePerRack = 32 * 1024 * 1024;
			conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));
			// 初始化自己的配置管理
			DataDealConfiguration.create(outputPath, conf);
			
			Job job = Job.getInstance(conf, "FilterSubway" + ":");
			job.setJarByClass(FilterSubwayMain.class);
			job.setMapperClass(mmeMaper.class);
			job.setReducerClass(mmeReduce.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			int reduceNum = getReduceNum(conf);
			job.setNumReduceTasks(reduceNum);
			
			if (!job.waitForCompletion(true))
			{
				System.out.println("FilterSubway Job error! stop run.");
				throw (new Exception("system.exit1"));
			}
		}catch(Exception e){
			e.printStackTrace();
		}	
	}
	
	public static int getReduceNum(Configuration conf) throws Exception{
		long inputDirSize = 0;
		int reduceNum = 1;
		HDFSOper hdfsOper = null;	
		
		hdfsOper = new HDFSOper(conf);
		
		if (hdfsOper.checkDirExist(inputPath))
		{
			System.out.println("mulu cunzai");
			inputDirSize += hdfsOper.getSizeOfPath(inputPath, false);
		}
		if (inputDirSize > 0)
		{
			double sizeG = inputDirSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = 2;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
		}
		return reduceNum;
	}

}
