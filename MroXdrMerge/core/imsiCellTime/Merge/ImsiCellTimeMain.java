package imsiCellTime.Merge;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import imsiCellTime.Merge.ImsiCellTimeMapper.ImsiCellTimeMap;
import imsiCellTime.Merge.ImsiCellTimeMapper.ImsiPartitioner;
import imsiCellTime.Merge.ImsiCellTimeReduce.ImsiCellTimeReducer;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import util.MaprConfHelper;

public class ImsiCellTimeMain
{
	protected static final Log LOG = LogFactory.getLog(ImsiCellTimeMain.class);
	private static int reduceNum;
	private static String queueName;
	private static String ImeiCellTime_inputPath;
	private static String dayNum; // 合并几天的数据
	private static String filterTimes;// 平均时间少于filterTimes不写出
	private static String outpath_table;
	private static String outpath;
	private static String mergedOutPutPath;

	public static void main(String args[])
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

	private static void makeConfig(Configuration conf, String[] args)
	{
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		ImeiCellTime_inputPath = args[2];
		dayNum = args[3];//
		if (Integer.parseInt(dayNum) <= 0)
		{
			System.out.println("dayNum  must  biger  than  " + dayNum);
			return;
		}
		filterTimes = args[4];
		outpath_table = args[5];

		// table output path
		outpath = outpath_table + "/output";
		mergedOutPutPath = outpath_table + "/ImsiResidentCell";

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		conf.set("mastercom.mroxdrmerge.mergedOutPutPath", mergedOutPutPath);
		conf.set("mastercom.dayNum", dayNum);
		conf.set("mastercom.filterTimes", filterTimes);
		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");// default
		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.job.jvm.numtasks", "-1");// jvm可以执行多个map
		MaprConfHelper.CustomMaprParas(conf);

		// 将小文件进行整合
		long splitMinSize = 128 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
		long minsizePerNode = 10 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
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
		if (args.length != 6)
		{
			System.err.println(
					"input not enough reduceNum/queueName /ImeiCellTime_inputPath/dayNum/filterTimes/outpath_table");
			throw (new IOException("ImsiCellTimeMain args input error!"));
		}
		makeConfig(conf, args);
		HDFSOper hdfsOper = null;
		Job job = Job.getInstance(conf, "ImsiCellTimesMerge");
		job.setNumReduceTasks(reduceNum);
		job.setJarByClass(ImsiCellTimeMain.class);
		job.setReducerClass(ImsiCellTimeReducer.class);
		job.setMapOutputKeyClass(ImeiCellTimesKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(ImsiPartitioner.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;

		if (!ImeiCellTime_inputPath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			String[] inpaths = ImeiCellTime_inputPath.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (hdfsOper.checkDirExist(tm_inpath))
				{
					MultipleInputs.addInputPath(job, new Path(tm_inpath), CombineTextInputFormat.class,
							ImsiCellTimeMap.class);
				}
				inputSize += hdfsOper.getSizeOfPath(tm_inpath, false);
			}
		}
		else
		{
			String[] inpaths = ImeiCellTime_inputPath.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				MultipleInputs.addInputPath(job, new Path(tm_inpath), CombineTextInputFormat.class,
						ImsiCellTimeMap.class);
			}
		}

		if (inputSize > 0)
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
		if (reduceNum >= 5000)
		{
			reduceNum = 5000;
		}
		job.setNumReduceTasks(reduceNum);
		MultipleOutputs.addNamedOutput(job, "ImsiCellTimes", TextOutputFormat.class, NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		String tarPath = "";
		if (!outpath_table.contains(":"))
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		else
			new File(outpath_table)
					.renameTo(new File(outpath_table + (new SimpleDateFormat("yyMMddHHmmss").format(new Date()))));
		return job;
	}

}
