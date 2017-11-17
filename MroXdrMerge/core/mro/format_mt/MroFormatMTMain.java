package mro.format_mt;

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

import StructData.StaticConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mro.format_mt.MroFormatMapper.MroMapper;
import mro.format_mt.MroFormatMapper_sichuan.MroMapper_ERICSSON;
import mro.format_mt.MroFormatMapper_sichuan.MroMapper_HUAWEI_TD;
import mro.format_mt.MroFormatMapper_sichuan.MroMapper_NSN_TD;
import mro.format_mt.MroFormatMapper_sichuan.MroMapper_ZTE_TD;
import mro.format_mt.MroFormatReducer.StatReducer;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import util.MaprConfHelper;

public class MroFormatMTMain
{
	protected static final Log LOG = LogFactory.getLog(MroFormatMTMain.class);

	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String inpath_mro;
	private static String inpath_mre;
	private static String outpath_table;
	private static String outpath;
	private static String path_mroformat;
	private static String ERICSSON_path;
	private static String HUAWEI_path;
	private static String NSN_path;
	private static String ZTE_path;

	private static void makeConfig_home(Configuration conf, String[] args) throws Exception
	{// 1 NULL 01_170315
		// D:/Data/mrosc/ns/MR/ERICSSON/20170315,D:/Data/mrosc/ns/MR/HUAWEI/20170315,D:/Data/mrosc/ns/MR/NSN/20170315,D:/Data/mrosc/ns/MR/ZTE/20170315
		// NULL d:/data/outsc1
		reduceNum = 50;
		queueName = args[1];
		outpath_date = args[2];
		inpath_mro = args[3];
		inpath_mre = args[4];
		outpath_table = args[5];
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro))
		{
			String path[] = inpath_mro.split(",", -1);
			if (path.length < 4)
			{
				System.err.println("mro Path not right     [ERICSSON_path,HUAWEI_path,NSN_path,ZTE_path]");
				throw (new Exception("mro Path not right"));
			}
			ERICSSON_path = path[0];
			HUAWEI_path = path[1];
			NSN_path = path[2];
			ZTE_path = path[3];
		}

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_mroformat = outpath_table + "/mroformat_" + outpath_date;
		// path_tb_signal = outpath_table + "/tb_signal_nsamgsm_" +
		// outpath_date;

		LOG.info(outpath);
		LOG.info(path_mroformat);
		// LOG.info(path_tb_signal);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		conf.set("mastercom.mroxdrmerge.mroformat.path_mroformat", path_mroformat);

		MaprConfHelper.CustomMaprParas(conf);

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

		// 增加压缩
		// Should the outputs of the maps be compressed before being sent across
		// the network. Uses SequenceFile compression.
		// conf.set("mapreduce.map.output.compress","TRUE");
		// If the map outputs are compressed, how should they be compressed?
		// conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.Lz4Codec");

		// Java opts for the task processes. The following symbol, if present,
		// will be interpolated: @taskid@ is replaced by current TaskID. Any
		// other occurrences of '@' will go unchanged. For example, to enable
		// verbose gc logging to a file named for the taskid in /tmp and to set
		// the heap maximum to be a gigabyte, pass a 'value' of: -Xmx1024m
		// -verbose:gc -Xloggc:/tmp/@taskid@.gc Usage of -Djava.library.path can
		// cause programs to no longer function if hadoop native libraries are
		// used. These values should instead be set as part of LD_LIBRARY_PATH
		// in the map / reduce JVM env using the mapreduce.map.env and
		// mapreduce.reduce.env config settings.
		// conf.set("mapred.child.java.opts","-verbose:gc
		// -XX:+PriintGCDetails");

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.map.speculative", "false");// 停止推测功能

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
			// 中间过程压缩
			conf.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
			conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=/usr/local/hadoop/lzo/lib");
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
			System.err.println("Usage: MroFormat <reduce num> <queen type> <date time> <mro path> <mre path>  <result path>");
			System.err.println("Now error input num is : " + args.length);
			throw (new Exception("MroFormatMTMain args input error!"));
		}
		makeConfig_home(conf, args);
		String mroFilter = "";
		String mreFilter = "";

		if (args.length >= 7)
		{
			if (!args[6].toUpperCase().equals("NULL"))
			{
				mroFilter = args[6];
			}
		}

		// HDFSOper hdfsOper = new
		// HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
		// MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());

		HDFSOper hdfsOper = null;
		try
		{
			if (!inpath_mro.contains(":"))
			{
				hdfsOper = new HDFSOper(conf);
			}
		}
		catch (Exception e)
		{
			// System.out.println("hdfsOper error!");
		}

		Job job = Job.getInstance(conf, "MroXdrMerge.mroformat" + ":" + outpath_date);
		job.setNumReduceTasks(reduceNum);

		job.setJarByClass(MroFormatMTMain.class);
		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		String[] inpaths = null;
		if (!inpath_mro.contains(":"))
		{
			inpaths = inpath_mro.split(",", -1);
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false, mroFilter);
			}
		}
		if (!inpath_mre.contains(":") && !inpath_mre.equals("NULL"))
		{
			inpaths = inpath_mre.split(",", -1);
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false);
			}
		}

		if (inputSize > 0)
		{
			int dealReduceSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getDealSizeReduce());
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = dealReduceSize / 1024;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
			if (mroFilter.length() > 0)
			{
				reduceNum = Math.max((int) (sizeG * 10 / sizePerReduce), reduceNum);
			}
			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is " + reduceNum);
		}

		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro))
		{
			MultipleInputs.addInputPath(job, new Path(ERICSSON_path), CombineTextInputFormat.class, MroMapper_ERICSSON.class);
			MultipleInputs.addInputPath(job, new Path(HUAWEI_path), CombineTextInputFormat.class, MroMapper_HUAWEI_TD.class);
			MultipleInputs.addInputPath(job, new Path(NSN_path), CombineTextInputFormat.class, MroMapper_NSN_TD.class);
			MultipleInputs.addInputPath(job, new Path(ZTE_path), CombineTextInputFormat.class, MroMapper_ZTE_TD.class);
		}
		else
		{
			inpaths = inpath_mro.split(",", -1);
			for (String ip : inpaths)
			{
				if (hdfsOper != null && !hdfsOper.checkFileExist(ip))
				{
					System.err.println("Mro path is not exists : " + ip);
					continue;
				}
				MultipleInputs.addInputPath(job, new Path(ip), CombineTextInputFormat.class, MroMapper.class);
			}
		}

		if (!inpath_mre.contains(":") && !inpath_mre.equals("NULL"))
		{
			inpaths = inpath_mre.split(",", -1);
			for (String ip : inpaths)
			{
				if (ip.trim().length() == 0 || ip.trim().equals(StaticConfig.InputPath_NoData))
				{
					continue;
				}

				if (!hdfsOper.checkFileExist(ip))
				{
					System.err.println("Mre path is not exists : " + ip);
					continue;
				}
				MultipleInputs.addInputPath(job, new Path(ip), CombineTextInputFormat.class, MroMapper.class);
			}
		}

		MultipleOutputs.addNamedOutput(job, "mroformat", TextOutputFormat.class, NullWritable.class, Text.class);
		// MultipleOutputs.addNamedOutput(job, "tbsignal",
		// TextOutputFormat.class, NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		// 检测输出目录是否存在，存在就改名
		String tarPath = "";
		if (!outpath_table.contains(":"))
		{
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		}

		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
