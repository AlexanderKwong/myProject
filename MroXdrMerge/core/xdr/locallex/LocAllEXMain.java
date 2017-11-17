package xdr.locallex;

import java.io.File;

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

import cellconfig.CellConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.CombineSmallFileInputFormat;
import jan.com.hadoop.mapred.DataDealConfiguration;
import jan.com.hadoop.mapred.DataDealJob;
import jan.util.StringHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import util.MaprConfHelper;
import xdr.locallex.LocAllMapper.ImsiPartitioner;
import xdr.locallex.LocAllMapper.ImsiSortKeyComparator;
import xdr.locallex.LocAllMapper.ImsiSortKeyGroupComparator;
import xdr.locallex.LocAllMapper.UserLocMapper_MRLOC;
import xdr.locallex.LocAllMapper.UserLocMapper_XDRLOC;
import xdr.locallex.LocAllMapper.XdrDataMapper;
import xdr.locallex.LocAllReducer.StatReducer;
import xdr.locallex.model.XdrDataFactory;

public class LocAllEXMain
{
	protected static final Log LOG = LogFactory.getLog(LocAllEXMain.class);

	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String outpath_table;

	private static String inpath_mrloc;
	private static String inpath_xdrloc;
	private static int inpathCount;
	private static int[] inpathTypes;
	private static String[] inpaths;

	private static int outpathCount;
	private static int[] outpathTypes;
	private static String[] outpathIndexs;
	private static String[] outpaths;
	private static String outpath;

	// output table
	private static String path_TB_EVENT_MID_IN_SAMPLE;
	private static String path_TB_EVENT_MID_OUT_SAMPLE;
	private static String path_TB_EVENT_LOW_IN_SAMPLE;
	private static String path_TB_EVENT_LOW_OUT_SAMPLE;
	private static String path_TB_EVENT_HIGH_IN_SAMPLE;
	private static String path_TB_EVENT_HIGH_OUT_SAMPLE;

	private static String path_TB_EVENT_HIGH_OUT_GRID;
	private static String path_TB_EVENT_MID_OUT_GRID;
	private static String path_TB_EVENT_LOW_OUT_GRID;

	private static String path_TB_EVENT_HIGH_OUT_CELLGRID;
	private static String path_TB_EVENT_MID_OUT_CELLGRID;
	private static String path_TB_EVENT_LOW_OUT_CELLGRID;

	public static String path_TB_EVENT_HIGH_IN_GRID;
	public static String path_TB_EVENT_MID_IN_GRID;
	private static String path_TB_EVENT_LOW_IN_GRID;

	private static String path_TB_EVENT_HIGH_IN_CELLGRID;
	private static String path_TB_EVENT_MID_IN_CELLGRID;
	private static String path_TB_EVENT_LOW_IN_CELLGRID;

	// build
	public static String path_TB_EVENT_HIGH_BUILD_GRID;
	public static String path_TB_EVENT_MID_BUILD_GRID;
	private static String path_TB_EVENT_LOW_BUILD_GRID;

	private static String path_TB_EVENT_HIGH_BUILD_CELLGRID;
	private static String path_TB_EVENT_MID_BUILD_CELLGRID;
	private static String path_TB_EVENT_LOW_BUILD_CELLGRID;

	private static String path_TB_EVENT_CELL;

	// 高铁场景
	private static String path_TB_EVENT_AREA;
	private static String path_TB_EVENT_AREA_CELL;
	private static String path_TB_EVENT_AREA_CELLGRID;
	private static String path_TB_EVENT_AREA_GRID;

	public static long allSize = 0;

	private static void makeConfig(Configuration conf, String[] args)
	{
		int index = 0;
		reduceNum = Integer.parseInt(args[index++]);
		queueName = args[index++];
		outpath_date = args[index++];
		inpath_xdrloc = args[index++];
		inpath_mrloc = args[index++];
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
		outpath = outpath_table + "/output";

		String outpath = outpath_date;

		outpath = outpath.replace("01_", "");

		path_TB_EVENT_MID_IN_SAMPLE = outpath_table + "/tb_evt_insample_mid_dd_" + outpath;
		path_TB_EVENT_MID_OUT_SAMPLE = outpath_table + "/tb_evt_outsample_mid_dd_" + outpath;
		path_TB_EVENT_LOW_IN_SAMPLE = outpath_table + "/tb_evt_insample_low_dd_" + outpath;
		path_TB_EVENT_LOW_OUT_SAMPLE = outpath_table + "/tb_evt_outsample_low_dd_" + outpath;
		path_TB_EVENT_HIGH_IN_SAMPLE = outpath_table + "/tb_evt_insample_high_dd_" + outpath;
		path_TB_EVENT_HIGH_OUT_SAMPLE = outpath_table + "/tb_evt_outsample_high_dd_" + outpath;

		path_TB_EVENT_HIGH_OUT_GRID = outpath_table + "/tb_evt_outgrid_high_dd_" + outpath;
		path_TB_EVENT_MID_OUT_GRID = outpath_table + "/tb_evt_outgrid_mid_dd_" + outpath;
		path_TB_EVENT_LOW_OUT_GRID = outpath_table + "/tb_evt_outgrid_low_dd_" + outpath;

		path_TB_EVENT_HIGH_OUT_CELLGRID = outpath_table + "/tb_evt_outgrid_cell_high_dd_" + outpath;
		path_TB_EVENT_MID_OUT_CELLGRID = outpath_table + "/tb_evt_outgrid_cell_mid_dd_" + outpath;
		path_TB_EVENT_LOW_OUT_CELLGRID = outpath_table + "/tb_evt_outgrid_cell_low_dd_" + outpath;

		path_TB_EVENT_HIGH_IN_GRID = outpath_table + "/tb_evt_ingrid_high_dd_" + outpath;
		path_TB_EVENT_MID_IN_GRID = outpath_table + "/tb_evt_ingrid_mid_dd_" + outpath;
		path_TB_EVENT_LOW_IN_GRID = outpath_table + "/tb_evt_ingrid_low_dd_" + outpath;

		path_TB_EVENT_HIGH_IN_CELLGRID = outpath_table + "/tb_evt_ingrid_cell_high_dd_" + outpath;
		path_TB_EVENT_MID_IN_CELLGRID = outpath_table + "/tb_evt_ingrid_cell_mid_dd_" + outpath;
		path_TB_EVENT_LOW_IN_CELLGRID = outpath_table + "/tb_evt_ingrid_cell_low_dd_" + outpath;

		// build
		path_TB_EVENT_HIGH_BUILD_GRID = outpath_table + "/tb_evt_building_high_dd_" + outpath;
		path_TB_EVENT_MID_BUILD_GRID = outpath_table + "/tb_evt_building_mid_dd_" + outpath;
		path_TB_EVENT_LOW_BUILD_GRID = outpath_table + "/tb_evt_building_low_dd_" + outpath;

		path_TB_EVENT_HIGH_BUILD_CELLGRID = outpath_table + "/tb_evt_building_cell_high_dd_" + outpath;
		path_TB_EVENT_MID_BUILD_CELLGRID = outpath_table + "/tb_evt_building_cell_mid_dd_" + outpath;
		path_TB_EVENT_LOW_BUILD_CELLGRID = outpath_table + "/tb_evt_building_cell_low_dd_" + outpath;

		path_TB_EVENT_CELL = outpath_table + "/tb_evt_cell_dd_" + outpath;

		// 高铁场景统计
		path_TB_EVENT_AREA = outpath_table + "/tb_evt_area_dd_" + outpath;
		path_TB_EVENT_AREA_CELL = outpath_table + "/tb_evt_area_cell_dd_" + outpath;

		path_TB_EVENT_AREA_CELLGRID = outpath_table + "/tb_evt_area_outgrid_cell_dd_" + outpath;
		path_TB_EVENT_AREA_GRID = outpath_table + "/tb_evt_area_outgrid_dd_" + outpath;

		// make config
		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		// set table
		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_MID_IN_SAMPLE", path_TB_EVENT_MID_IN_SAMPLE);
		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_MID_OUT_DTSAMPLE", path_TB_EVENT_MID_OUT_SAMPLE);
		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_LOW_IN_SAMPLE", path_TB_EVENT_LOW_IN_SAMPLE);
		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_LOW_OUT_SAMPLE", path_TB_EVENT_LOW_OUT_SAMPLE);
		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_HIGH_IN_SAMPLE", path_TB_EVENT_HIGH_IN_SAMPLE);
		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_HIGH_OUT_DTSAMPLE", path_TB_EVENT_HIGH_OUT_SAMPLE);

		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_OUT_GRID", path_TB_EVENT_HIGH_OUT_GRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_OUT_GRID", path_TB_EVENT_MID_OUT_GRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_OUT_GRID", path_TB_EVENT_LOW_OUT_GRID);

		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_OUT_CELLGRID", path_TB_EVENT_HIGH_OUT_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_OUT_CELLGRID", path_TB_EVENT_MID_OUT_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_OUT_CELLGRID", path_TB_EVENT_LOW_OUT_CELLGRID);

		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_IN_GRID", path_TB_EVENT_HIGH_IN_GRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_IN_GRID", path_TB_EVENT_MID_IN_GRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_IN_GRID", path_TB_EVENT_LOW_IN_GRID);

		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_IN_CELLGRID", path_TB_EVENT_HIGH_IN_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_IN_CELLGRID", path_TB_EVENT_MID_IN_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_IN_CELLGRID", path_TB_EVENT_LOW_IN_CELLGRID);

		// build
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_BUILD_GRID", path_TB_EVENT_HIGH_BUILD_GRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_BUILD_GRID", path_TB_EVENT_MID_BUILD_GRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_BUILD_GRID", path_TB_EVENT_LOW_BUILD_GRID);

		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_BUILD_CELLGRID", path_TB_EVENT_HIGH_BUILD_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_BUILD_CELLGRID", path_TB_EVENT_MID_BUILD_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_BUILD_CELLGRID", path_TB_EVENT_LOW_BUILD_CELLGRID);

		conf.set("mastercom.mroxdrmerge.locall.TB_EVENT_CELL", path_TB_EVENT_CELL);

		// 高铁场景统计

		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA", path_TB_EVENT_AREA);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA_CELL", path_TB_EVENT_AREA_CELL);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA_CELLGRID", path_TB_EVENT_AREA_CELLGRID);
		conf.set("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA_GRID", path_TB_EVENT_AREA_GRID);

		String inpathindex = "";
		for (int i = 0; i < inpathCount; ++i)
		{
			inpathindex += inpathTypes[i] + ";" + inpaths[i] + "\\$";
		}
		inpathindex = StringHelper.SideTrim(inpathindex, "\\$");
		conf.set("mastercom.mroxdrmerge.locall.inpathindex", inpathindex);

		String outputindex = "";
		for (int i = 0; i < outpathCount; ++i)
		{
			outputindex += outpathTypes[i] + ";" + outpathIndexs[i] + ";" + outpaths[i] + "\\$";
		}
		outputindex = StringHelper.SideTrim(outputindex, "\\$");
		conf.set("mastercom.mroxdrmerge.locall.outpathindex", outputindex);

		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");//
		// default0.05

		MaprConfHelper.CustomMaprParas(conf);

		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		conf.set("mapreduce.map.speculative", "false");// 停止推测功能

		// 将小文件进行整合
		long minsize = Long.parseLong(MainModel.GetInstance().getAppConfig().getDealSizeMap()) * 1024 * 1024;
		long splitMinSize = minsize;
		long splitMaxSize = minsize * 2;
		conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));
		conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMaxSize));

		long minsizePerNode = minsize / 2;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
		long minsizePerRack = minsize / 2;
		conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			// root.bdoc.renter_1.renter_9.dev_571 80ca02329e5bdfd0bc79
			// facd3668468f2b4bb3e4064b9d88ea414ac3acfc
			conf.set("mapreduce.job.queuename", "root.bdoc.renter_1.renter_9.dev_571");
			conf.set("hadoop.security.bdoc.access.id", "80ca02329e5bdfd0bc79");
			conf.set("hadoop.security.bdoc.access.key", "facd3668468f2b4bb3e4064b9d88ea414ac3acfc");
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
		makeConfig(conf, args);

		Job job = Job.getInstance(conf, "XDRLocAll" + ":" + outpath_date);

		job.setJarByClass(LocAllEXMain.class);
		job.setReducerClass(StatReducer.class);
		job.setMapOutputKeyClass(ImsiKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(ImsiSortKeyComparator.class);
		job.setPartitionerClass(ImsiPartitioner.class);
		job.setGroupingComparatorClass(ImsiSortKeyGroupComparator.class);

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

			// xdr loc
			String[] tm_inpaths = inpath_xdrloc.split(",");
			for (int j = 0; j < tm_inpaths.length; ++j)
			{
				if (hdfsOper.checkDirExist(tm_inpaths[j]))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpaths[j], false);
				}
			}

			// mr loc
			tm_inpaths = inpath_mrloc.split(",");
			for (int j = 0; j < tm_inpaths.length; ++j)
			{
				if (hdfsOper.checkDirExist(tm_inpaths[j]))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpaths[j], false);
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
		// /////////////////////////////////////////////////////

		// input
		if (hdfsOper != null && hdfsOper.checkDirExist(inpath_xdrloc))
		{
			LOG.info("find xdrloc location path : " + inpath_xdrloc);
			MultipleInputs.addInputPath(job, new Path(inpath_xdrloc), CombineTextInputFormat.class, UserLocMapper_XDRLOC.class);
		}

		if (hdfsOper != null && hdfsOper.checkDirExist(inpath_mrloc))
		{
			LOG.info("find mrloc location path : " + inpath_mrloc);
			MultipleInputs.addInputPath(job, new Path(inpath_mrloc), CombineTextInputFormat.class, UserLocMapper_MRLOC.class);
		}

		for (int i = 0; i < inpathCount; ++i)
		{
			String[] tm_inpaths = inpaths[i].split(",");

			for (int j = 0; j < tm_inpaths.length; ++j)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpaths[j]))
				{
					System.out.println("[info]input path is exists : " + tm_inpaths[j]);

					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, XdrDataMapper.class);
				}
				else if (hdfsOper == null && new File(tm_inpaths[j]).exists())
				{
					MultipleInputs.addInputPath(job, new Path(tm_inpaths[j]), CombineSmallFileInputFormat.class, XdrDataMapper.class);
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

		MultipleOutputs.addNamedOutput(job, "tbEventMidInSample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidOutSample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowInSample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowOutSample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventHighInSample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventHighOutSample", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbEventHighOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbEventHighOutCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidOutCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowOutCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbEventHighInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowInGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbEventHighInCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidInCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowInCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		// build
		MultipleOutputs.addNamedOutput(job, "tbEventHighBuildGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidBuildGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowBuildGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbEventHighBuildCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventMidBuildCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tbEventLowBuildCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbEventCell", TextOutputFormat.class, NullWritable.class, Text.class);

		// 高铁场景统计
		MultipleOutputs.addNamedOutput(job, "tbArea", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbAreaCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbAreaGridCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tbAreaGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		String tarPath = "";
		if (!outpath_table.contains(":"))
		{
			HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
		}

		return job;
	}

	public static void main(String[] args)
	{
		try
		{
			localMain(args);
		}
		catch (Exception e)
		{

			e.printStackTrace();
		}
	}

	public static void localMain(String[] args) throws Exception
	{

		String queueName = args[0];// network
		String statTime = args[1];// 01_151013

		Configuration conf = new Configuration();

		MainModel.GetInstance().setConf(conf);
//		conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
//		CellConfig.GetInstance().loadLteCell(conf);
		HDFSOper hdfsOper = new HDFSOper(conf);
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();

		int pcnt = 0;
		String[] myArgs = new String[2500];
		myArgs[pcnt++] = "100";
		myArgs[pcnt++] = queueName;
		myArgs[pcnt++] = statTime;
		// 位置库的数据
		// /user/gaowei/mt_wlyh/Data/mroxdrmerge/mro_loc/data_01_170830/TB_LOC_LIB_01_170830
		//

		String xdr_locPath = String.format("%s/mro_loc/data_%s/XDR_LOC_LIB_%s", mroXdrMergePath, statTime, statTime);
		String mro_locPath = String.format("%s/mro_loc/data_%s/TB_LOC_LIB_%s", mroXdrMergePath, statTime, statTime);
		if (hdfsOper.checkDirExist(xdr_locPath))
		{
			System.out.println("xdr_locPath: " + xdr_locPath);
			allSize = allSize + hdfsOper.getHdfs().getContentSummary(new Path(xdr_locPath)).getLength();
		}
		System.out.println(mro_locPath);
		myArgs[pcnt++] = xdr_locPath;
		myArgs[pcnt++] = mro_locPath;

		myArgs[pcnt++] = String.format("%s/xdr_locall/data_%s", mroXdrMergePath, statTime);

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{

			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			// String[] hours = new String[]{"10"};
			String[] minuts = new String[] { "00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55" };
			// //////////////////////////////////// input
			myArgs[pcnt++] = "1153";
			// myArgs[pcnt++] = "49";
			// /user/wangjun/S_O_DPI_LTE_S1U_HTTP/load_time_d=20170830/load_time_h=10/load_time_m=00/part-m-00025
			for (int i = 0; i < hours.length; i++)
			{
				for (int j = 0; j < minuts.length; j++)
				{
					String mmePath = "/user/wangjun/S_O_DPI_LTE_S1_MME/load_time_d=20" + statTime.substring(3) + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";

					String httpPath = "/user/wangjun/S_O_DPI_LTE_S1U_HTTP/load_time_d=20" + statTime.substring(3) + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
					String mgPath = "/user/wangjun/S_O_DPI_VL_MG/load_time_d=20" + statTime.substring(3) + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";

					String svPath = "/user/wangjun/S_O_DPI_VL_SV/load_time_d=20" + statTime.substring(3) + "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";

					myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MME;
					myArgs[pcnt++] = mmePath;

					myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
					myArgs[pcnt++] = httpPath;

					myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MG;
					myArgs[pcnt++] = mgPath;

					myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_SV;
					myArgs[pcnt++] = svPath;
				}
			}
			String rtpPath = "/user/gaowei/rtp/20" + statTime.substring(3) + "";
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_RTP;
			myArgs[pcnt++] = rtpPath;

			// //////////////////////////////////// output

			myArgs[pcnt++] = "5";
			//
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MME;
			myArgs[pcnt++] = String.format("tbEventOriginS1mme");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1MME_%2$s", mroXdrMergePath, statTime);
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			myArgs[pcnt++] = String.format("tbEventOriginS1Http");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1HTTP_%2$s", mroXdrMergePath, statTime);
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MG;
			myArgs[pcnt++] = String.format("tbEventOriginS1Mg");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1MG_%2$s", mroXdrMergePath, statTime);
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_SV;
			myArgs[pcnt++] = String.format("tbEventOriginS1NeiMengSv");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1NEIMENGSV_%2$s", mroXdrMergePath, statTime);
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_RTP;
			myArgs[pcnt++] = String.format("tbEventOriginS1rtp");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1RTP_%2$s", mroXdrMergePath, statTime);
		}

		// ///////////////////////////////////////// 北京
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) || MainModel.GetInstance().getCompile().Assert(CompileMark.HaErBin)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi) || MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)
				)
		{
			// //////////////////////////////////// input

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{
				myArgs[pcnt++] = "10";
			}
			else
			{
				myArgs[pcnt++] = "7";// input path count
			}

			// http是哪个路径
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			String httpPath = MainModel.GetInstance().getAppConfig().getHttpPath();
			myArgs[pcnt++] = String.format(httpPath, statTime.substring(3));

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MW;
			String mwPath = MainModel.GetInstance().getAppConfig().getMwPath();
			myArgs[pcnt++] = String.format(mwPath, statTime.substring(3));
			//
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_SV;
			String svPath = MainModel.GetInstance().getAppConfig().getSvPath();
			myArgs[pcnt++] = String.format(svPath, statTime.substring(3));
			//
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_RX;
			String rxPath = MainModel.GetInstance().getAppConfig().getRxPath();
			myArgs[pcnt++] = String.format(rxPath, statTime.substring(3));

			// hdfs://10.224.230.146:9000/mt_wlyh/Data/NoSatisUser/data_01_170725/Mgos_01_170725
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MOS_BEIJING;
			String mosPath = MainModel.GetInstance().getAppConfig().getMosPath();
			myArgs[pcnt++] = String.format(mosPath, mroXdrMergePath, statTime);

			// hdfs://10.224.230.146:9000/mt_wlyh/Data/NoSatisUser/data_01_170725/WJtdh_01_170725
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_WJTDH_BEIJING;
			String wjtDh = MainModel.GetInstance().getAppConfig().getDhwjtPath();
			myArgs[pcnt++] = String.format(wjtDh, mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MME;
			String mmePath = MainModel.GetInstance().getAppConfig().getMmePath();
			myArgs[pcnt++] = String.format(mmePath, statTime.substring(3));

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{

				myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_IMS_MO;
				String imsMoPath = MainModel.GetInstance().getAppConfig().getImsMoPath();
				myArgs[pcnt++] = String.format(imsMoPath, statTime.substring(3));

				myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_IMS_MT;
				String imsMtPath = MainModel.GetInstance().getAppConfig().getImsMtPath();
				myArgs[pcnt++] = String.format(imsMtPath, statTime.substring(3));

				myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_CDR_QUALITY;
				String qualityPaath = MainModel.GetInstance().getAppConfig().getQuaLityPath();
				myArgs[pcnt++] = String.format(qualityPaath, statTime.substring(3));
			} // //////////////////////////////////// output
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{
				myArgs[pcnt++] = "10";// output path count
			}
			else
			{
				myArgs[pcnt++] = "7";// output path count
			}

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			myArgs[pcnt++] = String.format("tbEventOriginS1uHttp");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1U_HTTP_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MW;
			myArgs[pcnt++] = String.format("tbEventOriginMw");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MW_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_SV;
			myArgs[pcnt++] = String.format("tbEventOriginSv");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_SV_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_RX;
			myArgs[pcnt++] = String.format("tbEventOriginRx");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Rx_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MOS_BEIJING;
			myArgs[pcnt++] = String.format("tbEventOriginMosBeiJing");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MOS_BEIJING_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_WJTDH_BEIJING;
			myArgs[pcnt++] = String.format("tbEventOriginMjtdhBeiJing");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MJTDH_BEIJING_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MME;
			myArgs[pcnt++] = String.format("tbEventOriginMme");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MME_%2$s", mroXdrMergePath, statTime);

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{

				myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_IMS_MO;
				myArgs[pcnt++] = String.format("tbEventOriginImsMo");
				myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Ims_Mo_%2$s", mroXdrMergePath, statTime);
				myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_IMS_MT;
				myArgs[pcnt++] = String.format("tbEventOriginImsMt");
				myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Ims_Mt_%2$s", mroXdrMergePath, statTime);
				myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_CDR_QUALITY;
				myArgs[pcnt++] = String.format("tbEventOriginCdrQuality");
				myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Cdr_Quality_%2$s", mroXdrMergePath, statTime);

			}

		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
		{

		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
		{
			myArgs[pcnt++] = "3";

			// input
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			String httpPath = MainModel.GetInstance().getAppConfig().getHttpPath();
			myArgs[pcnt++] = String.format(httpPath, statTime.substring(3));

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MME;
			String mmePath = MainModel.GetInstance().getAppConfig().getMmePath();
			myArgs[pcnt++] = String.format(mmePath, statTime.substring(3));

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_Uu;
			String uuPath = MainModel.GetInstance().getAppConfig().getUuPath();
			myArgs[pcnt++] = String.format(uuPath, statTime.substring(3));

			// output
			myArgs[pcnt++] = "3";

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			myArgs[pcnt++] = String.format("tbEventOriginS1uHttp");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1U_HTTP_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_MME;
			myArgs[pcnt++] = String.format("tbEventOriginS1mme");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1MME_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_Uu;
			myArgs[pcnt++] = String.format("tbEventOriginUu");
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_UU_%2$s", mroXdrMergePath, statTime);
		}
		else
		{
			// /////////////////////////////////////////// input

			myArgs[pcnt++] = "1";// input path count
			// 输入路径和数据
			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			myArgs[pcnt++] = String.format("%1$s/xdr_loc/data_%2$s/TB_4G_SIGNAL_CELL_%2$s", mroXdrMergePath, statTime);

			// /////////////////////////////////////////// output

			myArgs[pcnt++] = "1";// output path count

			myArgs[pcnt++] = "" + XdrDataFactory.XDR_DATATYPE_HTTP;
			myArgs[pcnt++] = String.format("tbEventOriginS1uHttp");
			// 输出路径和数据
			myArgs[pcnt++] = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1U_HTTP_%2$s", mroXdrMergePath, statTime);
		}

		// ///////////////////////////////////////////////////////////////////////////////////////////////////
		String[] params = new String[pcnt];
		for (int i = 0; i < pcnt; ++i)
		{
			params[i] = myArgs[i];
		}

		Job curJob = CreateJob(conf, params);

		DataDealJob dataJob = new DataDealJob(curJob, hdfsOper);
		if (!dataJob.Work())
		{
			System.out.println("locall job error! stop run.");
			System.exit(1);
		}

	}

}
