package imsiResident.locfill.broadband;

import imsiResident.locfill.broadband.ImsiResidentMappers.BroadBandMapper;
import imsiResident.locfill.broadband.ImsiResidentMappers.ImsiResidentMapper;
import imsiResident.locfill.broadband.ImsiResidentMappers.PhoneNumPartitioner;
import imsiResident.locfill.broadband.ImsiResidentReducers.ImsiResidentBroadbandReducer;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.HdfsHelper;
import util.MaprConfHelper;

public class ImsiResidentBroadbandLocMain
{
	protected static final Log LOG = LogFactory.getLog(ImsiResidentBroadbandLocMain.class);
	private static String queueName;
	private static String imsiResidentInput;
	private static String broadbandInput;
	private static String outpath_table;
	private static String outpath;
	
	private static void makeConfig(Configuration conf, String[] args)
	{
		queueName = args[0];
		imsiResidentInput = args[1];
		broadbandInput = args[2];
		outpath = args[3];
		
		// table output path
		outpath_table = outpath + "/ImsiResident";
		
		if(!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");// default
		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.job.jvm.numtasks", "-1");// jvm可以执行多个map
		MaprConfHelper.CustomMaprParas(conf);
		
		//将小文件进行整合
		long splitMinSize = 128 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
		long minsizePerNode = 10 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));
		
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
			// 中间过程压缩
			conf.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
			conf.set("mapreduce.map.output.compress",
					"LD_LIBRARY_PATH=" + MainModel.GetInstance().getAppConfig().getLzoPath());
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");	
		}
		// 初始化自己的配置管理
		DataDealConfiguration.create(outpath_table, conf);			
	}
	
	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if(args.length != 4)
		{
			System.err.println(
					"input not enough reduceNum/queueName/ImsiResident_inputPath/outpath_table");
			throw new IllegalArgumentException("ImsiResidentMain args input error!"); 
		}
		makeConfig(conf, args);
		HDFSOper hdfsOper = null;
		Job job = Job.getInstance(conf, "ImsiResidentLoc");
		job.setJarByClass(ImsiResidentBroadbandLocMain.class);
		job.setReducerClass(ImsiResidentBroadbandReducer.class);
		job.setMapOutputKeyClass(MobilePhoneNumKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(PhoneNumPartitioner.class);
		
		//set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		
		inputSize += addInputs(job, imsiResidentInput, ImsiResidentMapper.class);
		
		inputSize += addInputs(job, broadbandInput, BroadBandMapper.class);
		
		if(inputSize > 0)
		{
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = 2;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
			
			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is : " + reduceNum);
		}
		else
		{
			reduceNum = 1;
		}
		if(reduceNum >= 5000)
		{
			reduceNum = 5000;
		}
		job.setNumReduceTasks(reduceNum);
		FileOutputFormat.setOutputPath(job, new Path(outpath_table));

		renameExistOutput(job, outpath_table);
		return job;
	}	
	
	private static int addInputs(Job job, String inputs, Class mapperClass) throws Exception{
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		HDFSOper hdfsOper = null;
		
		int totalSize = 0;
		
		String[] inputArr = inputs.split(",");
		for(String in : inputArr){
			Path inPath = new Path(in);
			if(fs.exists(inPath)){
				MultipleInputs.addInputPath(job, inPath, CombineTextInputFormat.class, mapperClass);
				if(fs instanceof DistributedFileSystem){
					if(hdfsOper == null)
						hdfsOper = new HDFSOper(conf);
					totalSize += hdfsOper.getSizeOfPath(in, false);	
				}
			}
		}
		
		return totalSize;
	}
	
	private static void renameExistOutput(Job job, String output) throws IOException{
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		Path outPath = new Path(output);
		if(fs.exists(outPath))
			fs.rename(outPath, new Path(output + (new SimpleDateFormat("yyMMddHHmmss").format(new Date()))));
	}
	
	public static void main(String[] args){
		try
		{
			Job job = CreateJob(args);
			job.waitForCompletion(true);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
