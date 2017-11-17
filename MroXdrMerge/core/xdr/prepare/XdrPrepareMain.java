package xdr.prepare;

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
import jan.com.hadoop.mapred.TmpFileFilter;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import util.MaprConfHelper;
import xdr.prepare.XdrPrepareMapper.XdrDataMapper_HTTP;
import xdr.prepare.XdrPrepareMapper.XdrDataMapper_MME;
import xdr.prepare.XdrPrepareReducer.StatReducer;

public class XdrPrepareMain
{
	protected static final Log LOG = LogFactory.getLog(XdrPrepareMain.class);

	public static int reduceNum;
	public static String queueName;
	public static String outpath_date;
	public static String inpath_mme;
	public static String inpath_http;
	public static String outpath_table;
	public static String outpath;
	public static String path_ImsiCount;
	public static String path_ImsiIP;
	public static String path_Location;

	private static void makeConfig(Configuration conf, String[] args)
	{// 1 NULL 01_170316 d:/Data/s1mme-sc d:/Data/s1u-sc d:/output
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2];
		inpath_mme = args[3];
		inpath_http = args[4];
		outpath_table = args[5];

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_ImsiCount = outpath_table + "/TB_IMSI_COUNT_" + outpath_date;
		path_ImsiIP = outpath_table + "/TB_IMSI_IP_" + outpath_date;
		path_Location = outpath_table + "/TB_LOCATION_" + outpath_date;

		LOG.info(outpath);
		LOG.info(path_ImsiCount);
		LOG.info(path_ImsiIP);
		LOG.info(path_Location);
		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		conf.set("mastercom.mroxdrmerge.xdrprepare.path_ImsiCount", path_ImsiCount);
		conf.set("mastercom.mroxdrmerge.xdrprepare.path_ImsiIP", path_ImsiIP);
		conf.set("mastercom.mroxdrmerge.xdrprepare.path_Location", path_Location);

		MaprConfHelper.CustomMaprParas(conf);

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.map.speculative", "false");// 停止推测功能

		long minsize = Long.parseLong(MainModel.GetInstance().getAppConfig().getDealSizeMap()) * 1024 * 1024;

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
			conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=" + MainModel.GetInstance().getAppConfig().getLzoPath());
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		}
		// 初始化自己的配置管理
		DataDealConfiguration.create(outpath_table, conf);
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		return CreateJob(conf, args);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		if (args.length < 6)
		{
			System.err.println("Usage: XdrPrepare <in-mro> <in-xdr> <sample tbname> <event tbname>");
			throw (new Exception("XdrPrepare args input error!"));
		}
		makeConfig(conf, args);

		String mmeFilter = "";
		String httpFilter = "";

		if (args.length >= 8)
		{
			if (!args[6].toUpperCase().equals("NULL"))
			{
				mmeFilter = args[6];
			}

			if (!args[7].toUpperCase().equals("NULL"))
			{
				httpFilter = args[7];
			}
		}

		Job job = Job.getInstance(conf, "MroXdrMerge.mroxdr.xdrprepare" + ":" + outpath_date);
		job.setNumReduceTasks(reduceNum);

		job.setJarByClass(XdrPrepareMain.class);
		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		String[] inpaths = inpath_mme.split(",", -1);
		HDFSOper hdfsOper = null;

		if (!inpath_http.contains(":"))
		{
			// 检测输出目录是否存在，存在就改名
			hdfsOper = new HDFSOper(conf);

			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false, mmeFilter);
			}

			inpaths = inpath_http.split(",", -1);
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false, httpFilter);
			}

			if (inputSize > 0)
			{
				int dealReduceSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getDealSizeReduce());
				double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
				double sizePerReduce = dealReduceSize / 1024.0;
				reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
				LOG.info("total input size of data is : " + sizeG + " G ");
				LOG.info("the count of reduce to go is " + reduceNum);
			}
		}
		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		inpaths = inpath_mme.split(",", -1);
		for (String ip : inpaths)
		{
			if (ip.contains(":") || hdfsOper.checkDirExist(ip))
			{
				MultipleInputs.addInputPath(job, new Path(ip), CombineTextInputFormat.class, XdrDataMapper_MME.class);
			}
		}

		inpaths = inpath_http.split(",", -1);
		for (String ip : inpaths)
		{
			if (ip.contains(":") || hdfsOper.checkDirExist(ip))
			{
				MultipleInputs.addInputPath(job, new Path(ip), CombineTextInputFormat.class, XdrDataMapper_HTTP.class);
			}
		}

		MultipleOutputs.addNamedOutput(job, "imsicount", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsiip", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "location", TextOutputFormat.class, NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		String tarPath = "";
		if (hdfsOper != null)
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);

		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
