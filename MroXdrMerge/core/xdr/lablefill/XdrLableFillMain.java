package xdr.lablefill;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cellconfig.FilterCellConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import util.MaprConfHelper;
import xdr.lablefill.XdrLableMapper.ImsiCellTimeMap;
import xdr.lablefill.XdrLableMapper.ImsiPartitioner;
import xdr.lablefill.XdrLableMapper.ImsiSortKeyComparator;
import xdr.lablefill.XdrLableMapper.ImsiSortKeyGroupComparator;
import xdr.lablefill.XdrLableMapper.LableMapper;
import xdr.lablefill.XdrLableMapper.LocationMapper;
import xdr.lablefill.XdrLableMapper.LocationWFMapper;
import xdr.lablefill.XdrLableMapper.XdrDataMapper_23G;
import xdr.lablefill.XdrLableMapper.XdrDataMapper_HTTP;
import xdr.lablefill.XdrLableMapper.XdrDataMapper_MME;

public class XdrLableFillMain
{
	protected static final Log LOG = LogFactory.getLog(XdrLableFillMain.class);

	private static int reduceNum;
	public static String queueName;
	public static String inpath_xdr_mme;
	public static String inpath_xdr_http;
	public static String inpath_xdr_23g;
	public static String inpath_lable;
	public static String inpath_location;
	public static String inpath_locationWF;
	public static String inpath_imsicount;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String path_sample;
	public static String path_event;
	public static String path_event_mme;
	public static String path_event_http;
	public static String path_cell;
	public static String path_cellgrid;
	public static String path_cellgrid_23g;
	public static String path_grid;
	public static String path_grid_hour;
	public static String path_grid_user_hour;
	public static String path_grid_23g;
	public static String path_ImsiSampleIndex;
	public static String path_ImsiEventIndex;
	public static String path_xdrLoc;
	public static String path_xdrMore;
	public static String path_xdrLocation;
	public static String path_grid_dt;
	public static String path_grid_dt_23g;
	public static String path_grid_cqt;
	public static String path_grid_cqt_23g;
	public static String path_cellgrid_cqt_23g;
	public static String path_event_dt;
	public static String path_event_dt_23g;
	public static String path_event_dtex;
	public static String path_event_dtex_23g;
	public static String path_event_cqt;
	public static String path_event_cqt_23g;
	public static String path_event_index_dt;
	public static String path_event_index_cqt;
	public static String path_userinfo;
	public static String path_useract;
	public static String path_event_err;
	public static String path_event_err_23g;
	// user cell hour Times 2017.6.22
	public static String userCellHourTimePath;// 用户在小区每个小时停留时间和上报的经纬度

	// 用户常住小区位置配置表 20170626
	public static String path_ImsiCellLocPath = "";// 用户常驻小区配置路径

	public static String path_loc_lib;// 位置库

	public static String path_special_user_event;// 特例用户

	public static String path_xdrHiRailPath;// 高铁用户xdr

	private static void makeConfig_home(Configuration conf, String[] args)
	{// 1 NULL 01_170316 d:/Data/mme-bj d:/Data/http-bj NULL NULL NULL NULL NULL
		// d:/Data/outputbj
		// conf.set("hbase.zookeeper.quorum", "master,node001,node002");
		// conf.set("hbase.zookeeper.property.clientPort","2181");

		// conf.set("mapreduce.map.output.compress", "true");
		// conf.set("mapreduce.map.output.compress.codec",
		// "org.apache.hadoop.io.compress.Lz4Codec");

		reduceNum = Integer.parseInt(args[0]);
		queueName = args[1];
		outpath_date = args[2];
		inpath_xdr_mme = args[3];
		inpath_xdr_http = args[4];
		inpath_xdr_23g = args[5];
		inpath_lable = args[6];
		inpath_location = args[7];
		inpath_locationWF = args[8];
		inpath_imsicount = args[9];
		outpath_table = args[10];
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EstiLoc))
		{
			path_ImsiCellLocPath = MainModel.GetInstance().getAppConfig().getPath_ImsiCellLocPath();
		}

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_sample = outpath_table + "/TB_SIGNAL_SAMPLE_" + outpath_date;
		path_event = outpath_table + "/TB_SIGNAL_EVENT_" + outpath_date;
		path_event_mme = outpath_table + "/TB_SIGNAL_MMEXDR_" + outpath_date;
		path_event_http = outpath_table + "/TB_SIGNAL_HTTPXDR_" + outpath_date;
		path_cell = outpath_table + "/TB_SIGNAL_CELL_" + outpath_date;
		path_cellgrid = outpath_table + "/TB_SIGNAL_CELLGRID_" + outpath_date;
		path_cellgrid_23g = outpath_table + "/TB_23G_SIGNAL_CELLGRID_" + outpath_date;
		path_grid = outpath_table + "/TB_SIGNAL_GRID_" + outpath_date;
		path_grid_hour = outpath_table + "/TB_SIGNAL_GRID_HOUR_" + outpath_date;
		path_grid_user_hour = outpath_table + "/TB_SIGNAL_GRID_USER_HOUR_" + outpath_date;
		path_grid_23g = outpath_table + "/TB_23G_SIGNAL_GRID_" + outpath_date;
		path_ImsiSampleIndex = outpath_table + "/TB_SIGNAL_INDEX_SAMPLE_" + outpath_date;
		path_ImsiEventIndex = outpath_table + "/TB_SIGNAL_INDEX_EVENT_" + outpath_date;
		path_xdrLoc = outpath_table + "/XDR_LOC_" + outpath_date;
		path_xdrMore = outpath_table + "/XDR_MORE_" + outpath_date;
		path_xdrLocation = outpath_table + "/XDR_LOCATION_" + outpath_date;

		path_grid_dt = outpath_table + "/TB_DTSIGNAL_GRID_" + outpath_date;
		path_grid_dt_23g = outpath_table + "/TB_23G_DTSIGNAL_GRID_" + outpath_date;
		path_grid_cqt = outpath_table + "/TB_CQTSIGNAL_GRID_" + outpath_date;
		path_grid_cqt_23g = outpath_table + "/TB_23G_CQTSIGNAL_GRID_" + outpath_date;
		path_cellgrid_cqt_23g = outpath_table + "/TB_23G_CQTSIGNAL_CELLGRID_" + outpath_date;
		path_event_dt = outpath_table + "/TB_DTSIGNAL_EVENT_" + outpath_date;
		path_event_dt_23g = outpath_table + "/TB_23G_DTSIGNAL_EVENT_" + outpath_date;
		path_event_dtex = outpath_table + "/TB_DTEXSIGNAL_EVENT_" + outpath_date;
		path_event_dtex_23g = outpath_table + "/TB_23G_DTEXSIGNAL_EVENT_" + outpath_date;
		path_event_cqt = outpath_table + "/TB_CQTSIGNAL_EVENT_" + outpath_date;
		path_event_cqt_23g = outpath_table + "/TB_23G_CQTSIGNAL_EVENT_" + outpath_date;
		path_event_index_dt = outpath_table + "/TB_DTSIGNAL_INDEX_EVENT_" + outpath_date;
		path_event_index_cqt = outpath_table + "/TB_CQTSIGNAL_INDEX_EVENT_" + outpath_date;

		path_userinfo = outpath_table + "/TB_SIGNAL_USERINFO_" + outpath_date;
		path_useract = outpath_table + "/TB_SIG_USER_BEHAVIOR_LOC_CELL_" + outpath_date.substring(3);

		path_event_err = outpath_table + "/TB_ERRSIGNAL_EVENT_" + outpath_date;

		path_event_err_23g = outpath_table + "/TB_23G_ERRSIGNAL_EVENT_" + outpath_date;

		userCellHourTimePath = outpath_table + "/TB_USER_CELL_HOUR_TIMES_" + outpath_date;

		path_loc_lib = outpath_table + "/TB_LOC_LIB_" + outpath_date;
		path_special_user_event = outpath_table + "/TB_EVT_VAP_" + outpath_date;
		path_xdrHiRailPath = outpath_table + "/TB_XDR_HIRAIL_" + outpath_date;

		LOG.info(path_sample);
		LOG.info(path_event);
		LOG.info(path_event_http);
		LOG.info(path_event_mme);
		LOG.info(path_cell);
		LOG.info(path_cellgrid);
		LOG.info(path_grid);
		LOG.info(path_grid_hour);
		LOG.info(path_ImsiSampleIndex);
		LOG.info(path_ImsiEventIndex);
		LOG.info(path_xdrLoc);
		LOG.info(path_xdrMore);

		LOG.info(path_grid_dt);
		LOG.info(path_grid_cqt);
		LOG.info(path_event_dt);
		LOG.info(path_event_dt_23g);
		LOG.info(path_event_dtex);
		LOG.info(path_event_dtex_23g);
		LOG.info(path_event_cqt);
		LOG.info(path_event_cqt_23g);
		LOG.info(path_event_index_dt);
		LOG.info(path_event_index_cqt);

		LOG.info(path_userinfo);
		LOG.info(path_useract);
		LOG.info(path_grid_user_hour);
		LOG.info(path_event_err);
		LOG.info(path_event_err_23g);
		LOG.info(userCellHourTimePath);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		conf.set("mastercom.mroxdrmerge.xdr.locfill.inpath_imsicount", inpath_imsicount);

		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_sample", path_sample);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event", path_event);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_mme", path_event_mme);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_http", path_event_http);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cell", path_cell);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid", path_cellgrid);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_23g", path_cellgrid_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid", path_grid);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_hour", path_grid_hour);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_user_hour", path_grid_user_hour);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_23g", path_grid_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_ImsiSampleIndex", path_ImsiSampleIndex);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_ImsiEventIndex", path_ImsiEventIndex);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_xdrLoc", path_xdrLoc);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_xdrMore", path_xdrMore);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_xdrLocation", path_xdrLocation);

		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt", path_grid_dt);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt_23g", path_grid_dt_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt", path_grid_cqt);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt_23g", path_grid_cqt_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_cqt_23g", path_cellgrid_cqt_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dt", path_event_dt);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dt_23g", path_event_dt_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex", path_event_dtex);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex_23g", path_event_dtex_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt", path_event_cqt);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt_23g", path_event_cqt_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_index_dt", path_event_index_dt);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_index_cqt", path_event_index_cqt);

		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_userinfo", path_userinfo);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_useract", path_useract);

		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_err", path_event_err);

		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_err_23g", path_event_err_23g);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.userCellHourTimePath", userCellHourTimePath);
		conf.set("mastercom.mroxdrmerge.xdr.locfill.path_loc_lib", path_loc_lib);
		conf.set("mastercom.mroxdrmerge.evt.vap", path_special_user_event);
		conf.set("mastercom.mroxdrmerge.path_xdrHiRailPath", path_xdrHiRailPath);

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

		// conf.set("mapreduce.job.jvm.numtasks", "1");
		conf.set("mapreduce.task.timeout", "1200000");// 默认600000 �?600s
		conf.set("mapreduce.reduce.speculative", "false");// 停止推测功能
		// conf.set("mapreduce.reduce.log.level", "ALL");//全量日志输出
		conf.set("mapreduce.job.jvm.numtasks", "-1");// jvm可以执行多个map

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
		if (args.length < 11)
		{
			System.err.println("Usage: xdr lable fill <in-mro> <in-xdr> <out path> <out table path> <out path date>");
			throw (new Exception("XdrLableFillMain args input error!"));
		}
		makeConfig_home(conf, args);
		String mmeFilter = "";
		String httpFilter = "";

		if (args.length >= 13)
		{
			if (!args[11].toUpperCase().equals("NULL"))
			{
				mmeFilter = args[11];
			}

			if (!args[12].toUpperCase().equals("NULL"))
			{
				httpFilter = args[12];
			}
		}

		// 检测输出目录是否存在，存在就改
		HDFSOper hdfsOper = null;
		Job job = Job.getInstance(conf, "MroXdrMerge.xdr.locfill" + ":" + outpath_date);

		job.setJarByClass(XdrLableFillMain.class);
		job.setReducerClass(XdrLableFileSeqReducer.XdrDataFileReducer.class);
		job.setSortComparatorClass(ImsiSortKeyComparator.class);
		job.setPartitionerClass(ImsiPartitioner.class);
		job.setGroupingComparatorClass(ImsiSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(ImsiTimeKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;

		String[] inpaths = inpath_xdr_mme.split(",", -1);
		if (!inpath_xdr_mme.contains(":"))
		{
		   // hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
			//		MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
			hdfsOper = new HDFSOper(conf);
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false, mmeFilter);
			}

			if (!inpath_xdr_http.equals("NULL"))
			{
				inpaths = inpath_xdr_http.split(",", -1);
				for (String tm_inpath_xdr : inpaths)
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false, httpFilter);
				}
			}
		}

		if (inputSize > 0)
		{
			int dealReduceSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getDealSizeReduce());
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			double sizePerReduce = dealReduceSize / 1024.0;
			
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.FilterCell) && FilterCellConfig.GetInstance().loadFilterCell(conf) && FilterCellConfig.GetInstance().getLteCellInfoList().size() > 0)
			{
				reduceNum = FilterCellConfig.GetInstance().getLteCellInfoList().size();
			}
			else
			{		
				reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
			}

			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is : " + reduceNum);
		}
		else
		{
			reduceNum = 1;
		}
		job.setNumReduceTasks(reduceNum);
		// /////////////////////////////////////////////////////

		inpaths = inpath_xdr_mme.split(",", -1);
		for (String tm_inpath_xdr : inpaths)
		{
			if (tm_inpath_xdr.contains(":") || hdfsOper.checkDirExist(tm_inpath_xdr))
			{
				MultipleInputs.addInputPath(job, new Path(tm_inpath_xdr), CombineTextInputFormat.class, XdrDataMapper_MME.class);
			}
		}

		if (!inpath_xdr_http.equals("NULL"))
		{
			inpaths = inpath_xdr_http.split(",", -1);
			for (String tm_inpath_xdr : inpaths)
			{
				if (tm_inpath_xdr.contains(":") || hdfsOper.checkDirExist(tm_inpath_xdr))
				{
					LOG.info("path exits : " + tm_inpath_xdr);
					MultipleInputs.addInputPath(job, new Path(tm_inpath_xdr), CombineTextInputFormat.class, XdrDataMapper_HTTP.class);
				}
				else
				{
					LOG.info("path no exits : " + tm_inpath_xdr);
				}
			}
		}

		if (!inpath_xdr_http.contains(":") && !inpath_xdr_23g.equals("NULL") && hdfsOper.checkDirExist(inpath_xdr_23g))
		{
			LOG.info("path exits : " + inpath_xdr_23g);
			MultipleInputs.addInputPath(job, new Path(inpath_xdr_23g), CombineTextInputFormat.class, XdrDataMapper_23G.class);
		}
		else
		{
			LOG.info("path no exits : " + inpath_xdr_23g);
		}

		if (!inpath_xdr_http.contains(":") && !inpath_lable.equals("NULL") && hdfsOper.checkDirExist(inpath_lable))
		{
			LOG.info("path exits : " + inpath_lable);
			MultipleInputs.addInputPath(job, new Path(inpath_lable), TextInputFormat.class, LableMapper.class);
		}
		else
		{
			LOG.info("path no exits : " + inpath_lable);
		}

		String locations[] = inpath_location.split(",", -1);
		for (String location : locations)
		{
			if (!location.contains(":") && !location.equals("NULL") && hdfsOper.checkDirExist(location) || location.contains(":"))
			{
				LOG.info("inpath_location path exits : " + inpath_location);
				MultipleInputs.addInputPath(job, new Path(location), CombineTextInputFormat.class, LocationMapper.class);
			}
			else
			{
				LOG.info(" inpath_location path no exits : " + location);
			}
		}

		if (!inpath_xdr_http.contains(":") && (!inpath_locationWF.equals("NULL") && hdfsOper.checkDirExist(inpath_locationWF)))
		{
			LOG.info("path exits : " + inpath_locationWF);
			MultipleInputs.addInputPath(job, new Path(inpath_locationWF), CombineTextInputFormat.class, LocationWFMapper.class);
		}
		else
		{
			LOG.info("path no exits : " + inpath_locationWF);
		}

		// 用户常驻小区配置表
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EstiLoc))
		{
			if (!path_ImsiCellLocPath.contains(":") && hdfsOper.checkDirExist(path_ImsiCellLocPath))
			{
				LOG.info("path exits : " + path_ImsiCellLocPath);
				MultipleInputs.addInputPath(job, new Path(path_ImsiCellLocPath), CombineTextInputFormat.class, ImsiCellTimeMap.class);
			}
			else if (path_ImsiCellLocPath.contains(":") && new File(path_ImsiCellLocPath).exists())
			{
				LOG.info("path exits : " + path_ImsiCellLocPath);
				MultipleInputs.addInputPath(job, new Path(path_ImsiCellLocPath), CombineTextInputFormat.class, ImsiCellTimeMap.class);
			}
			else
			{
				LOG.info("path not exits : " + path_ImsiCellLocPath);
			}
		}

		MultipleOutputs.addNamedOutput(job, "xdrsample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrevent", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrmmeevent", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrhttpevent", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrcell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrcellgrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrcellgrid23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrgrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrgridhour", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrgriduserhour", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrgrid23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsisampleindex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsieventindex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrloc", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrMore", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrLocation", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "griddt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "griddt23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gridcqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gridcqt23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "cellgridcqt23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventdt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventdt23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventdtex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventdtex23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventcqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventcqt23g", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventindexdt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "eventindexcqt", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "userinfo", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "useract", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "eventerr", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "eventerr23g", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "cellhourTime", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "loclib", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "evtVap", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "hirailxdr", TextOutputFormat.class, NullWritable.class, Text.class);

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
