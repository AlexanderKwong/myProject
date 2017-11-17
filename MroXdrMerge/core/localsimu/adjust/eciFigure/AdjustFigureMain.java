package localsimu.adjust.eciFigure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import localsimu.adjust.eciFigure.AdjustFigureMappers.EciTableConfigMap;
import localsimu.adjust.eciFigure.AdjustFigureReduce.AdjustFigureReducer;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;

public class AdjustFigureMain
{
	protected static final Log LOG = LogFactory.getLog(AdjustFigureMain.class);
	public static int reduceNum;
	public static String queueName;
	public static String inputPath;
	public static String outputSrcPath;// 输出根目录
	public static String outputMiddlePath;// 中间路径
	public static String outPath;// output的输出路径
	public static String adjustedPath;// 输出数据的存放路径
	public static int size = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSize());

	private static void makeConfig_home(Configuration conf, String[] args)
	{
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		inputPath = args[2];
		outputSrcPath = args[3];
		if (size == 10)
		{
			outputMiddlePath = outputSrcPath + "/adjusted/10";
		}
		else if (size == 40)
		{
			outputMiddlePath = outputSrcPath + "/adjusted/40";
		}
		outPath = outputMiddlePath + "/output";
		adjustedPath = outputMiddlePath + "/figureData";
		for (int i = 0; i < args.length; i++)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}
		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
		// conf set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.8");
		conf.set("mapreduce.task.io.sort.mb", "1024");
		int mapMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getMapMemory());
		int reduceMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getReduceMemory());
		conf.set("mapreduce.map.memory.mb", mapMemory + "");
		conf.set("mapreduce.reduce.memory.mb", reduceMemory + "");
		conf.set("mapreduce.map.java.opts", "-Xmx" + (int) (mapMemory * 0.8) + "M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx" + (int) (reduceMemory * 0.8) + "M");
		conf.set("mapreduce.reduce.cpu.vcores", MainModel.GetInstance().getAppConfig().getReduceVcore());
		conf.set("mapreduce.task.timeout", "1200000");
		long splitMinSize = 512 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));
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
		conf.set("mastercom.cellgrid.enbidtable.adjustedPath", adjustedPath);
		// 初始化自己的配置管理
		DataDealConfiguration.create(outputMiddlePath, conf);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		//xsh
		HDFSOper hdfsOper = new HDFSOper(conf);
//		HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//				MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
		if (args.length < 4)
		{
			System.err.println("Usage: AdjustFigure path not enough");
			throw (new Exception("AdjustFigureMain args input error!"));
		}
		makeConfig_home(conf, args);
		Job job = Job.getInstance(conf, "Cellgrid_AdjustFigure");
		job.setJarByClass(AdjustFigureMain.class);
		job.setReducerClass(AdjustFigureReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		String[] inpaths = null;
		job.setNumReduceTasks(reduceNum);
		if (!inputPath.equals("NULL"))
		{
			inpaths = inputPath.split(",|\t", -1);
			for (String inpath : inpaths)
			{
				if (hdfsOper.checkDirExist(inpath))
				{
					MultipleInputs.addInputPath(job, new Path(inpath), TextInputFormat.class, EciTableConfigMap.class);
				}
				else
				{
					LOG.info(inpath + " not exists : " + inpath);
				}
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		String tarPath = "";
		HdfsHelper.reNameExistsPath(hdfsOper, outputMiddlePath, tarPath);
		return job;
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		return CreateJob(conf, args);
	}

	public static void main(String[] args) throws Exception
	{
		Job job = CreateJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
