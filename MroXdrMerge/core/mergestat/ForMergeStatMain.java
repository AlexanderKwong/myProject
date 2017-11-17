package mergestat;

import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.CombineSmallFileInputFormat;
import jan.com.hadoop.mapred.DataDealConfiguration;
import jan.util.StringHelper;
import mergestat.MergeStatMapper.MergeMapper;
import mergestat.MergeStatReducer.StatReducer;
import util.HdfsHelper;
import util.MaprConfHelper;

public class ForMergeStatMain
{
	protected static final Log LOG = LogFactory.getLog(ForMergeStatMain.class);

	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String outpath_table;

	private static int inpathCount;
	private static int[] inpathTypes;
	private static String[] inpaths;

	private static int outpathCount;
	private static int[] outpathTypes;
	private static String[] outpathIndexs;
	private static String[] outpaths;

	//////////////
	private static String outpath;

	private static void makeConfig(Configuration conf, String[] args, int roundId)
	{
		int index = 0;
		reduceNum = Integer.parseInt(args[index++]);
		queueName = args[index++];
		outpath_date = args[index++];
		outpath_table = args[index++];

		inpathCount = Integer.parseInt(args[index++]);
		inpathTypes = new int[inpathCount];
		inpaths = new String[inpathCount];
		for (int i = 0; i < inpathCount; ++i)
		{
			inpathTypes[i] = Integer.parseInt(args[index++]);
			inpaths[i] = args[index++];
		}

		outpathCount = Integer.parseInt(args[index++]);
		outpathTypes = new int[outpathCount];
		outpathIndexs = new String[outpathCount];
		outpaths = new String[outpathCount];
		for (int i = 0; i < outpathCount; ++i)
		{
			outpathTypes[i] = Integer.parseInt(args[index++]);
			outpathIndexs[i] = args[index++];
			outpaths[i] = args[index++];
		}

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output" + roundId;

		// make config
		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		String inpathindex = "";
		for (int i = 0; i < inpathCount; ++i)
		{
			inpathindex += inpathTypes[i] + ";" + inpaths[i] + "\\$";
		}
		inpathindex = StringHelper.SideTrim(inpathindex, "\\$");
		conf.set("mastercom.mroxdrmerge.mergestat.inpathindex", inpathindex);

		String outputindex = "";
		for (int i = 0; i < outpathCount; ++i)
		{
			outputindex += outpathTypes[i] + ";" + outpathIndexs[i] + ";" + outpaths[i] + "\\$";
		}
		outputindex = StringHelper.SideTrim(outputindex, "\\$");
		conf.set("mastercom.mroxdrmerge.mergestat.outpathindex", outputindex);

		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");// default0.05

		// conf.set("yarn.app.mapreduce.am.resource.mb", "16000");// 设置AM占用内存大小

		MaprConfHelper.CustomMaprParas(conf);

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.map.speculative", "false");// 停止推测功能

		// 将小文件进行整合
		long splitMinSize = 128 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
		long minsizePerNode = 10 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));

		// 初始化自己的配置管理
		DataDealConfiguration.create(outpath_table, conf);
	}

	/**
	 * 
	 * @param conf
	 * @param args
	 * @param i
	 *            第几轮汇聚
	 * @return
	 * @throws Exception
	 */
	public static Job CreateJob(Configuration conf, String[] args, int roundId) throws Exception
	{
		// 检测输出目录是否存在，存在就改名

		makeConfig(conf, args, roundId);

		Job job = Job.getInstance(conf, "MroXdrMerge.mergestat" + ":" + outpath_date);
		job.setNumReduceTasks(reduceNum);

		job.setJarByClass(ForMergeStatMain.class);
		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(MergeKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		HDFSOper hdfsOper = null;
		if (!outpath_table.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			for (int i = 0; i < inpathCount; ++i)
			{
				String[] tm_inpaths = inpaths[i].split(",");

				for (int j = 0; j < tm_inpaths.length; ++j)
				{
					if (hdfsOper.checkDirExist(tm_inpaths[j]))
					{
						inputSize += hdfsOper.getSizeOfPath(tm_inpaths[j], false);
					}
				}
			}
		}

		if (inputSize > 0)
		{
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = 2;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);

			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is " + reduceNum);
		}

		job.setNumReduceTasks(reduceNum);
		///////////////////////////////////////////////////////

		// input
		for (int i = 0; i < inpathCount; ++i)
		{
			String[] tm_inpaths = inpaths[i].split(",");

			for (int j = 0; j < tm_inpaths.length; ++j)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpaths[j]))
				{
					// System.out.println("[info]input path is exists : " +
					// tm_inpaths[j]);

					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, MergeMapper.class);
				}
				else if (hdfsOper == null && new File(tm_inpaths[j]).exists())
				{
					// System.out.println("[info]input path is exists : " +
					// tm_inpaths[j]);
					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, MergeMapper.class);
				}
				else
				{
					System.err.println("[warn]input path is not exists : " + tm_inpaths[j]);
				}
			}

		}

		// output
		for (int i = 0; i < outpathCount; ++i)
		{
			MultipleOutputs.addNamedOutput(job, outpathIndexs[i], TextOutputFormat.class, NullWritable.class, Text.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		// String tarPath = "";
		// if (!outpath_table.contains(":"))
		// {
		// HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		// }

		return job;
	}

	// public static void main(String[] args) throws Exception
	// {
	// Job job = CreateJob(new Configuration(), args);
	// System.exit(job.waitForCompletion(true) ? 0 : 1);
	//
	// }

}
