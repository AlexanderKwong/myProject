package loc.imsifill;

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

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import loc.imsifill.LocImsiFillMapper.ImsiIPKeyComparator;
import loc.imsifill.LocImsiFillMapper.ImsiIPKeyGroupComparator;
import loc.imsifill.LocImsiFillMapper.ImsiIPMapper;
import loc.imsifill.LocImsiFillMapper.ImsiIPPartitioner;
import loc.imsifill.LocImsiFillMapper.LocationMapper;
import loc.imsifill.LocImsiFillReducer.StatReducer;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import util.MaprConfHelper;

public class LocImsiFillMain
{
	protected static final Log LOG = LogFactory.getLog(LocImsiFillMain.class);

	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String inpath_location;
	private static String inpath_imsiIP;
	private static String outpath_table;
	private static String outpath;
	private static String path_location;

	private static void makeConfig(Configuration conf, String[] args)
	{//1 NULL 01_170316 D:\Data\locnx D:\Data\http-nx d:/data/outputnx
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2];
		inpath_location = args[3];
		inpath_imsiIP = args[4];
		outpath_table = args[5];

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_location = outpath_table + "/TB_LOCATION_" + outpath_date;

		LOG.info(outpath);
		LOG.info(path_location);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		conf.set("mastercom.mroxdrmerge.loc.imsifill.path_location", path_location);

		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");// default
																		// 0.05
		MaprConfHelper.CustomMaprParas(conf);

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.map.speculative", "false");// 停止推测功能

		long minsize = 1024 * 1024 * 1024L;
		if (args.length >= 8 && !args[6].toUpperCase().equals("NULL") && !args[6].toLowerCase().equals(""))
		{// 按小时计算
			minsize = 50 * 1024 * 1024L;
		}
		// 将小文件进行整合
		long splitMinSize = minsize;
		conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));

		long splitMaxSize = minsize * 2;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

		long minsizePerNode = minsize / 2;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = minsize / 2;
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
		if (args.length < 6)
		{
			System.err.println("Usage: loc imsi fill input error");
			System.err.println("Now error input num is : " + args.length);
			throw (new Exception("LocImsiFillMain args input error!"));
		}
		makeConfig(conf, args);
		String mmeFilter = "";
		String httpFilter = "";

		if (args.length >= 8)
		{
			if (!args[6].toLowerCase().equals("NULL"))
			{
				mmeFilter = args[6];
			}

			if (!args[7].toLowerCase().equals("NULL"))
			{
				httpFilter = args[7];
			}
		}
		
		HDFSOper hdfsOper = new HDFSOper(conf);

		Job job = Job.getInstance(conf, "MroXdrMerge.loc.imsifill" + ":" + outpath_date);
		job.setNumReduceTasks(reduceNum);

		job.setJarByClass(LocImsiFillMain.class);
		job.setReducerClass(StatReducer.class);
		job.setSortComparatorClass(ImsiIPKeyComparator.class);
		job.setPartitionerClass(ImsiIPPartitioner.class);
		job.setGroupingComparatorClass(ImsiIPKeyGroupComparator.class);
		job.setMapOutputKeyClass(ImsiIPKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;

		String[] inpaths = inpath_location.split(",", -1);
		if (!inpath_location.contains(":"))
		{
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false);
			}
	
			inpaths = inpath_imsiIP.split(",", -1);
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false, httpFilter);
			}
		}

		if (inputSize > 0)
		{
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = 1;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);

			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is " + reduceNum);
		}

		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		inpaths = inpath_location.split(",", -1);
		for (String ip : inpaths)
		{
			MultipleInputs.addInputPath(job, new Path(ip), CombineTextInputFormat.class, LocationMapper.class);
		}

		inpaths = inpath_imsiIP.split(",", -1);
		for (String ip : inpaths)
		{
			if (ip.contains(":") || hdfsOper.checkDirExist(ip))
			{
				MultipleInputs.addInputPath(job, new Path(ip), CombineTextInputFormat.class, ImsiIPMapper.class);
			}
		}

		MultipleOutputs.addNamedOutput(job, "location", TextOutputFormat.class, NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		// 检测输出目录是否存在，存在就改名
		String tarPath = "";
		HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);

		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
