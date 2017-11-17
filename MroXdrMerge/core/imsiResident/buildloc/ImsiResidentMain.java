package imsiResident.buildloc;

import imsiResident.buildloc.ImsiResidentMapper.ImsiPartitioner;
import imsiResident.buildloc.ImsiResidentMapper.ImsiResidentMap;
import imsiResident.buildloc.ImsiResidentReducer.ImsiResidentReduce;
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
import org.apache.hadoop.fs.Path;
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

public class ImsiResidentMain
{
	protected static final Log LOG = LogFactory.getLog(ImsiResidentMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String ImsiResident_inputPath;
	private static String outpath_table;
	private static String outpath;
	private static String mergedOutPutPath;
	
	private static void makeConfig(Configuration conf, String[] args)
	{
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		ImsiResident_inputPath = args[2];
		outpath_table = args[3];
		
		// table output path
		outpath = outpath_table + "/output";
		mergedOutPutPath = outpath_table + "/ImsiResident";
		
		if(!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		conf.set("mastercom.mroxdrmerge.mergedOutPutPath", mergedOutPutPath);
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
			throw (new IOException("ImsiResidentMain args input error!")); 
		}
		makeConfig(conf, args);
		HDFSOper hdfsOper = null;
		Job job = Job.getInstance(conf, "ImsiResidentMerge");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(ImsiResidentMain.class);
		job.setReducerClass(ImsiResidentReduce.class);
		job.setMapOutputKeyClass(ImsiResidentKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(ImsiPartitioner.class);
		
		//set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		
		if(!ImsiResident_inputPath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			String[] inpaths = ImsiResident_inputPath.split(",", -1);
			for(String tm_inpath : inpaths)
			{
				if(hdfsOper.checkDirExist(tm_inpath))
				{
					MultipleInputs.addInputPath(job, new Path(tm_inpath), CombineTextInputFormat.class,
							ImsiResidentMap.class);
				}
				inputSize += hdfsOper.getSizeOfPath(tm_inpath, false);
			}
		}
		else
		{
			String[] inpaths = ImsiResident_inputPath.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				MultipleInputs.addInputPath(job, new Path(tm_inpath), CombineTextInputFormat.class,
						ImsiResidentMap.class);
			}
		}
		
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
		MultipleOutputs.addNamedOutput(job, "ImsiResident", TextOutputFormat.class, NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		String tarPath = "";
		if (!outpath_table.contains(":"))
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		else
			new File(outpath_table)
					.renameTo(new File(outpath_table + (new SimpleDateFormat("yyMMddHHmmss").format(new Date()))));
		return job;
	}	
	
	public static void main(String[] args)
	{		
		try
		{
			Job job = CreateJob(args);
			job.waitForCompletion(true);
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
