package localsimu.grid_eci_table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import localsimu.grid_eci_table.CreateGridEciTableMappers.CellGridMap;
import localsimu.grid_eci_table.CreateGridEciTableMappers.figureMap;
import localsimu.grid_eci_table.CreateGridEciTableReduce.CreateGridEciTableReducer;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;

public class CreateGridEciTableMain
{
	protected static final Log LOG = LogFactory.getLog(CreateGridEciTableMain.class);
	public static int reduceNum;
	public static String queueName;
	public static String InputPath_cellGrid;
	public static String InputPath_figure;
	public static String outputSrcPath;// 输出根目录
	public static String outputMiddlePath;// 中间路径
	public static String outPath;// output的输出路径
	public static String gridEciTablePath;
	public static int size = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSize());

	private static void makeConfig_home(Configuration conf, String[] args)
	{
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		InputPath_cellGrid = args[2];
		InputPath_figure = args[3];
		outputSrcPath = args[4];
		if (size == 10)
		{
			outputMiddlePath = outputSrcPath + "/GridEciTable/10";
		}
		else if (size == 40)
		{
			outputMiddlePath = outputSrcPath + "/GridEciTable/40";
		}
		outPath = outputMiddlePath + "/output";
		gridEciTablePath = outputMiddlePath + "/GridEciData";

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
		conf.set("mastercom.cellgrid.enbidtable.gridEciTablePath", gridEciTablePath);
		// 初始化自己的配置管理
		DataDealConfiguration.create(outputMiddlePath, conf);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		if (args.length < 5)
		{
			System.err.println("Usage: grid_eci_table path not enough");
			System.exit(2);
		}
		makeConfig_home(conf, args);
		//xsh
		HDFSOper hdfsOper = new HDFSOper(conf);
//		HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//				MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
		Job job = Job.getInstance(conf, "grid_eci_table");
		job.setJarByClass(CreateGridEciTableMain.class);
		job.setReducerClass(CreateGridEciTableReducer.class);
		job.setMapOutputKeyClass(GridKey.class);
		job.setMapOutputValueClass(Text.class);
		// set reduce num
		long inputSize = 0;
		String[] Inpaths = null;

		if (!InputPath_cellGrid.equals("NULL"))
		{
			Inpaths = InputPath_cellGrid.split(",", -1);
			for (String inpath : Inpaths)
			{
				if (hdfsOper.checkDirExist(inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(inpath, false);
					MultipleInputs.addInputPath(job, new Path(inpath), TextInputFormat.class, CellGridMap.class);
				}
				else
				{
					LOG.info(inpath + " not exists : " + inpath);
				}
			}
		}

		if (!InputPath_figure.equals("NULL"))
		{
			Inpaths = InputPath_figure.split(",", -1);
			for (String inpath : Inpaths)
			{
				if (hdfsOper.checkDirExist(inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(inpath, false);
					MultipleInputs.addInputPath(job, new Path(inpath), TextInputFormat.class, figureMap.class);
				}
				else
				{
					LOG.info(inpath + " not exists : " + inpath);
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
			job.setNumReduceTasks(reduceNum);
		}

		MultipleOutputs.addNamedOutput(job, "gridEciTable", TextOutputFormat.class, NullWritable.class, Text.class);
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
