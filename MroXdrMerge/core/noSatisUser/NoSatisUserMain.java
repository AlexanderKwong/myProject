package noSatisUser;

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
import mro.lablefill.CellTimeKey;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import noSatisUser.NoSatisUserMapper.CellPartitioner;
import noSatisUser.NoSatisUserMapper.GMosMapper;
import noSatisUser.NoSatisUserMapper.WjtdhMapper;
import noSatisUser.NoSatisUserMapper.XdrLocationMappers;
import noSatisUser.NoSatisUserReduce.NoSatisUserReducers;
import util.HdfsHelper;
import util.MaprConfHelper;

public class NoSatisUserMain
{
	protected static final Log LOG = LogFactory.getLog(NoSatisUserMain.class);
	private static int reduceNum;
	public static String queueName;
	public static String inpath_xdr;
	public static String inpath_wjtdh;
	public static String inpath_gmos;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;

	private static void makeConfig_home(Configuration conf, String[] args)
	{
		reduceNum = 100;
		queueName = args[1];
		outpath_date = args[2];
		inpath_xdr = args[3];
		inpath_wjtdh = args[4];
		inpath_gmos = args[5];
		outpath_table = String.format("%s/NoSatisUser/data_%s", MainModel.GetInstance().getAppConfig().getMroXdrMergePath(), outpath_date);

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		outpath = outpath_table + "/output";

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		conf.set("mapreduce.job.date", outpath_date);
		conf.set("mapreduce.job.oupath", outpath_table);

		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");// default
		MaprConfHelper.CustomMaprParas(conf);

		// 将小文件进行整合
		long splitMinSize = 2 * 1024 * 1024 * 1024L;
		conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));

		long splitMaxSize = splitMinSize * 2;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

		long minsizePerNode = 1000 * 1024 * 1024L;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));

		long minsizePerRack = 1000 * 1024 * 1024L;
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
			System.err.println("Usage: GL_Imsi <queueName> ,<outpath_date>,<inpath_xdr> ,<inpath_wjtdh>,<inpath_gmos> ,<outpath_table>");
			throw (new Exception("NoSatisUserMain args input error!"));
		}
		makeConfig_home(conf, args);
		String mrFilter = "";
		if (args.length >= 8)
		{
			if (!args[7].toUpperCase().equals("NULL") && !args[7].toUpperCase().equals(""))
			{
				mrFilter = args[7];
			}
		}
		// 检测输出目录是否存在，存在就改�
		HDFSOper hdfsOper = null;
		if (!outpath_table.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
		}
		Job job = Job.getInstance(conf, "NoSatisUserMain" + ":" + outpath_date);
		job.setJarByClass(NoSatisUserMain.class);

		job.setReducerClass(NoSatisUserReducers.class);
		job.setPartitionerClass(CellPartitioner.class);
		job.setMapOutputKeyClass(CellTimeKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		String[] inpaths;

		if (!inpath_xdr.equals("NULL"))
		{
			inpaths = inpath_xdr.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (!tm_inpath.contains(":") && hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false, mrFilter);
				}
				else
				{
					LOG.info("path not exists : " + tm_inpath);
				}
			}
		}

		if (!inpath_gmos.equals("NULL"))
		{
			inpaths = inpath_gmos.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (!tm_inpath.contains(":") && hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false, mrFilter);
				}
				else
				{
					LOG.info("path not exists : " + tm_inpath);
				}
			}
		}

		if (!inpath_wjtdh.equals("NULL"))
		{
			inpaths = inpath_wjtdh.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (!tm_inpath.contains(":") && hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false, mrFilter);
				}
				else
				{
					LOG.info("path not exists : " + tm_inpath);
				}
			}
		}

		if (inputSize > 0)
		{
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = 1;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);

			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is : " + reduceNum);
		}
		else
		{
			reduceNum = 1;
		}
		job.setNumReduceTasks(reduceNum);

		if (inpath_xdr.contains(":") || hdfsOper.checkDirExist(inpath_xdr))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_xdr), CombineTextInputFormat.class, XdrLocationMappers.class);
		}
		else
		{
			LOG.info("No xdrlocation data!");
		}

		if (inpath_gmos.contains(":") || hdfsOper.checkDirExist(inpath_gmos))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_gmos), CombineTextInputFormat.class, GMosMapper.class);
		}
		else
		{
			LOG.info("No mro data!");
		}

		if (inpath_wjtdh.contains(":") || hdfsOper.checkDirExist(inpath_wjtdh))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_wjtdh), CombineTextInputFormat.class, WjtdhMapper.class);
		}
		else
		{
			LOG.info("No mro data!");
		}

		MultipleOutputs.addNamedOutput(job, "wjtdh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gmos", TextOutputFormat.class, NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));
		String tarPath = "";
		if (!outpath_table.contains(":"))
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);

		return job;
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
