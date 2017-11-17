package localsimu.eci_cellgrid_table;

import java.util.HashMap;
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
import localsimu.eci_cellgrid_table.CreateEciCellGridTableMappers.CellGridMap;
import localsimu.eci_cellgrid_table.CreateEciCellGridTableMappers.EciIndexMap;
import localsimu.eci_cellgrid_table.CreateEciCellGridTableReduce.CreateEciCellGridTableReducer;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;

public class CreateEciCellGridTableMain
{
	protected static final Log LOG = LogFactory.getLog(CreateEciCellGridTableMain.class);
	public static int reduceNum;
	public static String queueName;
	public static String InputPath_eciIndex;
	public static String InputPath_cellGrid;
	public static String outputSrcPath;// 输出根目录
	public static String outputMiddlePath;// 中间路径
	public static String outPath;// output的输出路径
	public static String EciTablePath;
	public static int size = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSize());

	private static void makeConfig_home(Configuration conf, String[] args)
	{
		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		InputPath_eciIndex = args[2];
		InputPath_cellGrid = args[3];
		outputSrcPath = args[4];
		if (size == 10)
		{
			outputMiddlePath = outputSrcPath + "/EciCellGridTable/10";
		}
		else if (size == 40)
		{
			outputMiddlePath = outputSrcPath + "/EciCellGridTable/40";
		}
		outPath = outputMiddlePath + "/output";
		EciTablePath = outputMiddlePath + "/EciCellGridData";
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
		// int mapMemory =
		// Integer.parseInt(MainModel.GetInstance().getAppConfig().getMapMemory());
		// int reduceMemory =
		// Integer.parseInt(MainModel.GetInstance().getAppConfig().getReduceMemory());
		conf.set("mapreduce.map.memory.mb", "8192");
		conf.set("mapreduce.reduce.memory.mb", "8192");
		conf.set("mapreduce.map.java.opts", "-Xmx" + (int) (8192 * 0.8) + "M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx" + (int) (8192 * 0.8) + "M");
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
		conf.set("mastercom.cellgrid.enbidtable.EciTablePath", EciTablePath);
		// 初始化自己的配置管理
		DataDealConfiguration.create(outputMiddlePath, conf);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		if (args.length < 5)
		{
			System.err.println("Usage: createEciCellGridTable path not enough");
			throw (new Exception("CreateEciCellGridTableMain args input error!"));
		}
		makeConfig_home(conf, args);
		//xsh
		HDFSOper hdfsOper = new HDFSOper(conf);
//		HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//				MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
		Job job = Job.getInstance(conf, "Create_eci_table");
		job.setJarByClass(CreateEciCellGridTableMain.class);
		job.setReducerClass(CreateEciCellGridTableReducer.class);
		job.setMapOutputKeyClass(GridKey.class);
		job.setMapOutputValueClass(Text.class);

		String[] Inpaths = null;

		if (!InputPath_cellGrid.equals("NULL"))
		{
			Inpaths = InputPath_cellGrid.split(",", -1);
			for (String inpath : Inpaths)
			{
				if (hdfsOper.checkDirExist(inpath))
				{
					MultipleInputs.addInputPath(job, new Path(inpath), TextInputFormat.class, CellGridMap.class);
				}
				else
				{
					LOG.info(inpath + " not exists : " + inpath);
				}
			}
		}

		if (!InputPath_eciIndex.equals("NULL"))
		{
			Inpaths = InputPath_eciIndex.split(",", -1);
			for (String inpath : Inpaths)
			{
				if (hdfsOper.checkDirExist(inpath))
				{
					MultipleInputs.addInputPath(job, new Path(inpath), TextInputFormat.class, EciIndexMap.class);
				}
				else
				{
					LOG.info(inpath + " not exists : " + inpath);
				}
			}
		}
		// 加载eci配置表
		// String eciConfigTable =
		// MainModel.GetInstance().getAppConfig().getEciConfigPath();
		// EciTableConfig.GetInstance().loadEnbidTable(conf, eciConfigTable);
		// HashMap<Long, Integer> eciMap =
		// EciTableConfig.GetInstance().getEciConfigTableMap();
		// 注册输出表名
		// for (long eci : eciMap.keySet())
		// {
		// MultipleOutputs.addNamedOutput(job, eci + "", TextOutputFormat.class,
		// NullWritable.class, Text.class);
		// }
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
