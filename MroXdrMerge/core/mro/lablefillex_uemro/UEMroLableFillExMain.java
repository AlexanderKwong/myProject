package mro.lablefillex_uemro;

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
import mro.lablefill.CellTimeKey;
import mro.lablefill.MroLableMapper.CellPartitioner;
import mro.lablefill.MroLableMapper.CellSortKeyComparator;
import mro.lablefill.MroLableMapper.CellSortKeyGroupComparator;
import mro.lablefill.MroLableMapper.XdrLocationMapper;
import mro.lablefillex_uemro.UEMroLableFileReducer.MroDataFileReducers;
import mro.lablefillex_uemro.UEMroLableMapper.MroDataMappers;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import xdr.lablefill.MrBuild;
import xdr.lablefill.MrBuildCell;
import xdr.lablefill.MrBuildCellNc;
import xdr.lablefill.MrInGrid;
import xdr.lablefill.MrInGridCell;
import xdr.lablefill.MrInGridCellNc;
import xdr.lablefill.MrOutGrid;
import xdr.lablefill.MrOutGridCell;
import xdr.lablefill.MrOutGridCellNc;
import xdr.lablefill.MrStatCell;
import xdr.lablefill.TopicCellIsolated;

public class UEMroLableFillExMain
{
	protected static final Log LOG = LogFactory.getLog(UEMroLableFillExMain.class);

	private static int reduceNum;
	public static String queueName;

	public static String inpath_xdr;
	public static String inpath_mre;
	public static String inpath_mro;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String path_sample;
	public static String path_event;
	public static String path_cell;
	public static String path_cell_freq;
	public static String path_cellgrid;
	public static String path_grid;
	public static String path_ImsiSampleIndex;
	public static String path_ImsiEventIndex;
	public static String path_locMore;
	public static String path_mroMore;
	public static String path_myLog;

	public static String path_grid_dt;
	public static String path_grid_dt_freq;
	public static String path_grid_cqt;
	public static String path_grid_cqt_freq;
	public static String path_sample_dt;
	public static String path_sample_dtex;
	public static String path_sample_cqt;
	public static String path_sample_index_dt;
	public static String path_sample_index_cqt;

	public static String path_ten_grid;
	public static String path_ten_grid_dt;
	public static String path_ten_grid_dt_freq;
	public static String path_ten_grid_cqt;
	public static String path_ten_grid_cqt_freq;
	public static String path_ten_cellgrid;

	public static String path_useract_cell;

	/////////////////////// 指纹库定位输出路径///////////////////
	public static String fpath_sample;
	public static String fpath_event;
	public static String fpath_cell;
	public static String fpath_cell_freq;
	public static String fpath_cellgrid;
	public static String fpath_grid;
	public static String fpath_ImsiSampleIndex;
	public static String fpath_ImsiEventIndex;
	public static String fpath_locMore;
	public static String fpath_mroMore;
	public static String fpath_myLog;

	public static String fpath_grid_dt;
	public static String fpath_grid_dt_freq;
	public static String fpath_grid_cqt;
	public static String fpath_grid_cqt_freq;
	public static String fpath_sample_dt;
	public static String fpath_sample_dtex;
	public static String fpath_sample_cqt;
	public static String fpath_sample_index_dt;
	public static String fpath_sample_index_cqt;

	public static String fpath_ten_grid;
	public static String fpath_ten_grid_dt;
	public static String fpath_ten_grid_dt_freq;
	public static String fpath_ten_grid_cqt;
	public static String fpath_ten_grid_cqt_freq;
	public static String fpath_ten_cellgrid;

	public static String fpath_useract_cell;

	// 新版本MR统计
	public static String path_mr_outgrid;
	public static String path_mr_build;
	public static String path_mr_ingrid;
	public static String path_mr_build_cell;
	public static String path_mr_ingrid_cell;
	public static String path_mr_cell;
	public static String path_mr_outgrid_cell;
	public static String path_mr_cellpare;
	public static String path_mr_outgrid_cellpare;
	public static String path_mr_ingrid_cellpare;
	public static String path_mr_build_cellpare;
	public static String path_topic_cell_isolated;

	private static void makeConfig_home(Configuration conf, String[] args)
	{
		//1000 NULL 01_170808 D:/Data/shenzhen/location D:/Data/shenzhen/uemr D:/Data/shenzhen/out D:/Data/shenzhen/location

		reduceNum = 1000;
		queueName = args[1];
		outpath_date = args[2];
		inpath_xdr = args[3];
		inpath_mro = args[4];
		outpath_table = args[5];
		inpath_mre = args[6];

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_sample = outpath_table + "/TB_SIGNAL_SAMPLE_" + outpath_date;
		path_event = outpath_table + "/TB_SIGNAL_EVENT_" + outpath_date;
		path_cell = outpath_table + "/TB_SIGNAL_CELL_" + outpath_date;
		path_cell_freq = outpath_table + "/TB_FREQ_SIGNAL_CELL_" + outpath_date;
		path_cellgrid = outpath_table + "/TB_SIGNAL_CELLGRID_" + outpath_date;
		path_grid = outpath_table + "/TB_SIGNAL_GRID_" + outpath_date;
		path_ImsiSampleIndex = outpath_table + "/TB_SIGNAL_INDEX_SAMPLE_" + outpath_date;
		path_ImsiEventIndex = outpath_table + "/TB_SIGNAL_INDEX_EVENT_" + outpath_date;
		path_locMore = outpath_table + "/LOC_MORE_" + outpath_date;
		path_mroMore = outpath_table + "/MRO_MORE_" + outpath_date;
		path_myLog = outpath_table + "/MYLOG_" + outpath_date;

		path_grid_dt = outpath_table + "/TB_DTSIGNAL_GRID_" + outpath_date;
		path_grid_dt_freq = outpath_table + "/TB_FREQ_DTSIGNAL_GRID_" + outpath_date;
		path_grid_cqt = outpath_table + "/TB_CQTSIGNAL_GRID_" + outpath_date;
		path_grid_cqt_freq = outpath_table + "/TB_FREQ_CQTSIGNAL_GRID_" + outpath_date;
		path_sample_dt = outpath_table + "/TB_DTSIGNAL_SAMPLE_" + outpath_date;
		path_sample_dtex = outpath_table + "/TB_DTEXSIGNAL_SAMPLE_" + outpath_date;
		path_sample_cqt = outpath_table + "/TB_CQTSIGNAL_SAMPLE_" + outpath_date;
		path_sample_index_dt = outpath_table + "/TB_DTSIGNAL_INDEX_SAMPLE_" + outpath_date;
		path_sample_index_cqt = outpath_table + "/TB_CQTSIGNAL_INDEX_SAMPLE_" + outpath_date;

		path_ten_grid = outpath_table + "/TB_SIGNAL_GRID10_" + outpath_date;
		path_ten_grid_dt = outpath_table + "/TB_DTSIGNAL_GRID10_" + outpath_date;
		path_ten_grid_dt_freq = outpath_table + "/TB_FREQ_DTSIGNAL_GRID10_" + outpath_date;
		path_ten_grid_cqt = outpath_table + "/TB_CQTSIGNAL_GRID10_" + outpath_date;
		path_ten_grid_cqt_freq = outpath_table + "/TB_FREQ_CQTSIGNAL_GRID10_" + outpath_date;
		path_ten_cellgrid = outpath_table + "/TB_SIGNAL_CELLGRID10_" + outpath_date;

		path_useract_cell = outpath_table + "/TB_SIG_USER_BEHAVIOR_LOC_MR_" + outpath_date.substring(3);

		///////////////// 指纹库定位生成路径/////////////////////
		fpath_sample = outpath_table + "/TB_FGSIGNAL_SAMPLE_" + outpath_date;
		fpath_event = outpath_table + "/TB_FGSIGNAL_EVENT_" + outpath_date;
		fpath_cell = outpath_table + "/TB_FGSIGNAL_CELL_" + outpath_date;
		fpath_cell_freq = outpath_table + "/TB_FGFREQ_SIGNAL_CELL_" + outpath_date;
		fpath_cellgrid = outpath_table + "/TB_FGSIGNAL_CELLGRID_" + outpath_date;
		fpath_grid = outpath_table + "/TB_FGSIGNAL_GRID_" + outpath_date;
		fpath_ImsiSampleIndex = outpath_table + "/TB_FGSIGNAL_INDEX_SAMPLE_" + outpath_date;
		fpath_ImsiEventIndex = outpath_table + "/TB_FGSIGNAL_INDEX_EVENT_" + outpath_date;
		fpath_locMore = outpath_table + "/LOC_FGMORE_" + outpath_date;
		fpath_mroMore = outpath_table + "/MRO_FGMORE_" + outpath_date;
		fpath_myLog = outpath_table + "/MYLOG_FG" + outpath_date;

		fpath_grid_dt = outpath_table + "/TB_FGDTSIGNAL_GRID_" + outpath_date;
		fpath_grid_dt_freq = outpath_table + "/TB_FGFREQ_DTSIGNAL_GRID_" + outpath_date;
		fpath_grid_cqt = outpath_table + "/TB_FGCQTSIGNAL_GRID_" + outpath_date;
		fpath_grid_cqt_freq = outpath_table + "/TB_FGFREQ_CQTSIGNAL_GRID_" + outpath_date;
		fpath_sample_dt = outpath_table + "/TB_FGDTSIGNAL_SAMPLE_" + outpath_date;
		fpath_sample_dtex = outpath_table + "/TB_FGDTEXSIGNAL_SAMPLE_" + outpath_date;
		fpath_sample_cqt = outpath_table + "/TB_FGCQTSIGNAL_SAMPLE_" + outpath_date;
		fpath_sample_index_dt = outpath_table + "/TB_FGDTSIGNAL_INDEX_SAMPLE_" + outpath_date;
		fpath_sample_index_cqt = outpath_table + "/TB_FGCQTSIGNAL_INDEX_SAMPLE_" + outpath_date;
		fpath_useract_cell = outpath_table + "/TB_FGSIG_USER_BEHAVIOR_LOC_MR_" + outpath_date.substring(3);

		fpath_ten_grid = outpath_table + "/TB_FGSIGNAL_GRID10_" + outpath_date;
		fpath_ten_grid_dt = outpath_table + "/TB_FGDTSIGNAL_GRID10_" + outpath_date;
		fpath_ten_grid_dt_freq = outpath_table + "/TB_FGFREQ_DTSIGNAL_GRID10_" + outpath_date;
		fpath_ten_grid_cqt = outpath_table + "/TB_FGCQTSIGNAL_GRID10_" + outpath_date;
		fpath_ten_grid_cqt_freq = outpath_table + "/TB_FGFREQ_CQTSIGNAL_GRID10_" + outpath_date;
		fpath_ten_cellgrid = outpath_table + "/TB_FGSIGNAL_CELLGRID10_" + outpath_date;

		// new mr stat
		path_mr_outgrid = outpath_table + "/TB_MR_OUTGRID_" + outpath_date.substring(3);
		path_mr_build = outpath_table + "/TB_MR_BUILD_" + outpath_date.substring(3);
		path_mr_ingrid = outpath_table + "/TB_MR_INGRID_" + outpath_date.substring(3);
		path_mr_build_cell = outpath_table + "/TB_MR_BUILD_CELL_" + outpath_date.substring(3);
		path_mr_ingrid_cell = outpath_table + "/TB_MR_INGRID_CELL_" + outpath_date.substring(3);
		path_mr_cell = outpath_table + "/TB_STAT_MR_CELL_" + outpath_date.substring(3);
		path_mr_outgrid_cell = outpath_table + "/TB_MR_OUTGRID_CELL_" + outpath_date.substring(3);
		path_mr_outgrid_cellpare = outpath_table + "/TB_MR_OUTGRID_CELLPARE_" + outpath_date.substring(3);
		path_mr_ingrid_cellpare = outpath_table + "/TB_MR_INGRID_CELLPARE_" + outpath_date.substring(3);
		path_mr_build_cellpare = outpath_table + "/TB_MR_BUILD_CELLPARE_" + outpath_date.substring(3);
		path_topic_cell_isolated = outpath_table + "/TB_TOPIC_CELL_ISOLATED_" + outpath_date;

		LOG.info(path_sample);
		LOG.info(path_event);
		LOG.info(path_cell);
		LOG.info(path_cell_freq);
		LOG.info(path_cellgrid);
		LOG.info(path_grid);
		LOG.info(path_ImsiSampleIndex);
		LOG.info(path_ImsiEventIndex);
		LOG.info(path_locMore);
		LOG.info(path_mroMore);
		LOG.info(path_myLog);

		LOG.info(path_grid_dt);
		LOG.info(path_grid_dt_freq);
		LOG.info(path_grid_cqt);
		LOG.info(path_grid_cqt_freq);
		LOG.info(path_sample_dt);
		LOG.info(path_sample_dtex);
		LOG.info(path_sample_cqt);
		LOG.info(path_sample_index_dt);
		LOG.info(path_sample_index_cqt);

		LOG.info(path_useract_cell);

		LOG.info(path_ten_grid);
		LOG.info(path_ten_grid_dt);
		LOG.info(path_ten_grid_dt_freq);
		LOG.info(path_ten_grid_cqt);
		LOG.info(path_ten_grid_cqt_freq);
		LOG.info(path_ten_cellgrid);
		///////////////// 指纹库定位生成路径/////////////////////
		LOG.info(fpath_sample);
		LOG.info(fpath_event);
		LOG.info(fpath_cell);
		LOG.info(fpath_cell_freq);
		LOG.info(fpath_cellgrid);
		LOG.info(fpath_grid);
		LOG.info(fpath_ImsiSampleIndex);
		LOG.info(fpath_ImsiEventIndex);
		LOG.info(fpath_locMore);
		LOG.info(fpath_mroMore);
		LOG.info(fpath_myLog);

		LOG.info(fpath_grid_dt);
		LOG.info(fpath_grid_dt_freq);
		LOG.info(fpath_grid_cqt);
		LOG.info(fpath_grid_cqt_freq);
		LOG.info(fpath_sample_dt);
		LOG.info(fpath_sample_dtex);
		LOG.info(fpath_sample_cqt);
		LOG.info(fpath_sample_index_dt);
		LOG.info(fpath_sample_index_cqt);
		LOG.info(fpath_ten_grid);
		LOG.info(fpath_ten_grid_dt);
		LOG.info(fpath_ten_grid_dt_freq);
		LOG.info(fpath_ten_grid_cqt);
		LOG.info(fpath_ten_grid_cqt_freq);
		LOG.info(fpath_useract_cell);
		LOG.info(fpath_ten_cellgrid);

		// new mr stat
		LOG.info(path_mr_outgrid);
		LOG.info(path_mr_build);
		LOG.info(path_mr_ingrid);
		LOG.info(path_mr_build_cell);
		LOG.info(path_mr_ingrid_cell);
		LOG.info(path_mr_cell);
		LOG.info(path_mr_outgrid_cell);
		LOG.info(path_mr_outgrid_cellpare);
		LOG.info(path_mr_ingrid_cellpare);
		LOG.info(path_mr_build_cellpare);
		LOG.info(path_topic_cell_isolated);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		// conf.set("hbase.zookeeper.quorum", "master,node001,node002");
		// conf.set("hbase.zookeeper.property.clientPort","2181");
		// conf.set("mapreduce.map.output.compress", "true");
		// conf.set("mapreduce.map.output.compress.codec",
		// "org.apache.hadoop.io.compress.Lz4Codec");

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample", path_sample);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_event", path_event);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cell", path_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cell_freq", path_cell_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid", path_cellgrid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid", path_grid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ImsiSampleIndex", path_ImsiSampleIndex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ImsiEventIndex", path_ImsiEventIndex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_myLog", path_myLog);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_locMore", path_locMore);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mroMore", path_mroMore);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt", path_grid_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt_freq", path_grid_dt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt", path_grid_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt_freq", path_grid_cqt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_dt", path_sample_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_dtex", path_sample_dtex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_cqt", path_sample_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_dt", path_sample_index_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_cqt", path_sample_index_cqt);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_useract_cell", path_useract_cell);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid", path_ten_grid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt", path_ten_grid_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt_freq", path_ten_grid_dt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt", path_ten_grid_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt_freq", path_ten_grid_cqt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_cellgrid", path_ten_cellgrid);
		///////////////// 指纹库定位生成路径/////////////////////
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_sample", fpath_sample);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_event", fpath_event);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_cell", fpath_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_cell_freq", fpath_cell_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_cellgrid", fpath_cellgrid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_grid", fpath_grid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ImsiSampleIndex", fpath_ImsiSampleIndex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ImsiEventIndex", fpath_ImsiEventIndex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_myLog", fpath_myLog);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_locMore", fpath_locMore);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_mroMore", fpath_mroMore);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_dt", fpath_grid_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_dt_freq", fpath_grid_dt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_cqt", fpath_grid_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_cqt_freq", fpath_grid_cqt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_dt", fpath_sample_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_dtex", fpath_sample_dtex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_cqt", fpath_sample_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_index_dt", fpath_sample_index_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_index_cqt", fpath_sample_index_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_useract_cell", fpath_useract_cell);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid", fpath_ten_grid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_dt", fpath_ten_grid_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_dt_freq", fpath_ten_grid_dt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_cqt", fpath_ten_grid_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_cqt_freq", fpath_ten_grid_cqt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_cellgrid", fpath_ten_cellgrid);

		// new mr stat
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid", path_mr_outgrid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_build", path_mr_build);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid", path_mr_ingrid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_build_cell", path_mr_build_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid_cell", path_mr_ingrid_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_cell", path_mr_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid_cell", path_mr_outgrid_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid_cellpare", path_mr_outgrid_cellpare);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid_cellpare", path_mr_ingrid_cellpare);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_build_cellpare", path_mr_build_cellpare);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_topic_cell_isolated", path_topic_cell_isolated);

		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");// default
		conf.set("mapreduce.task.io.sort.mb", "1024");
		// conf.set("mapreduce.map.memory.mb", "5120");
		// conf.set("mapreduce.reduce.memory.mb", "10240");
		// conf.set("mapreduce.map.java.opts", "-Xmx4096M");
		// conf.set("mapreduce.reduce.java.opts", "-Xmx8192M");
		int mapMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getMapMemory());
		int reduceMemory = Integer.parseInt(MainModel.GetInstance().getAppConfig().getReduceMemory());
		conf.set("mapreduce.map.memory.mb", mapMemory + "");
		conf.set("mapreduce.reduce.memory.mb", reduceMemory + "");
		conf.set("mapreduce.map.java.opts", "-Xmx" + (int) (mapMemory * 0.8) + "M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx" + (int) (reduceMemory * 0.8) + "M");
		conf.set("mapreduce.reduce.cpu.vcores", MainModel.GetInstance().getAppConfig().getReduceVcore());
		conf.set("mapreduce.task.timeout", "1200000");

		// The minimum size chunk that map input should be split into. Note that
		// some file formats may have minimum split sizes that take priority
		// over this setting.
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
		if (args.length != 7)
		{
			System.err.println("Usage: Mro_loc <in-mro> <in-xdr> <sample tbname> <event tbname>");
			throw (new Exception("UEMroLableFillExMain args input error!"));
		}
		makeConfig_home(conf, args);

		// 检测输出目录是否存在，存在就改�
		// HDFSOper hdfsOper = new
		// HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
		// MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());

		 HDFSOper hdfsOper = null;//new HDFSOper(conf);
		
		//xsh
//		HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//				MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());

		Job job = Job.getInstance(conf, "MroXdrMerge.mro.locfillex" + ":" + outpath_date);
		job.setNumReduceTasks(reduceNum);

		job.setJarByClass(UEMroLableFillExMain.class);
		job.setReducerClass(MroDataFileReducers.class);
		job.setSortComparatorClass(CellSortKeyComparator.class);
		job.setPartitionerClass(CellPartitioner.class);
		job.setGroupingComparatorClass(CellSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(CellTimeKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		String[] inpaths;
		if(!inpath_mro.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			if (!inpath_xdr.equals("NULL"))
			{
				
				inpaths = inpath_xdr.split(",", -1);
				for (String tm_inpath : inpaths)
				{
					if (hdfsOper.checkDirExist(tm_inpath))
					{
						inputSize += hdfsOper.getSizeOfPath(tm_inpath, false);
					}
					else
					{
						LOG.info("path not exists : " + tm_inpath);
					}
				}
			}
	
			inpaths = inpath_mro.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false);
				}
				else
				{
					LOG.info("path not exists : " + tm_inpath);
				}
			}
	
			inpaths = inpath_mre.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false);
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

		job.setNumReduceTasks(reduceNum);

		///////////////////////////////////////////////////////

		if (inpath_mro.contains(":") || hdfsOper.checkDirExist(inpath_xdr))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_xdr), TextInputFormat.class, XdrLocationMapper.class);
		}
		else
		{
			LOG.info("No xdrlocation data!");
		}

		if (inpath_mro.contains(":") ||  hdfsOper.checkDirExist(inpath_mro))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mro), TextInputFormat.class, MroDataMappers.class);
		}
		else
		{
			LOG.info("No mro data!");
		}

		if (inpath_mro.contains(":") || hdfsOper.checkDirExist(inpath_mre))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mre), TextInputFormat.class, MroDataMappers.class);
		}
		else
		{
			LOG.info("No mre data!");
		}

		MultipleOutputs.addNamedOutput(job, "mrosample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mroevent", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrocell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrocellfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrocellgrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrogrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsisampleindex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsieventindex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "myLog", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "locMore", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mroMore", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "griddt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "griddtfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gridcqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gridcqtfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampledt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampledtex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "samplecqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampleindexdt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampleindexcqt", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "useractcell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "tenmrogrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tengriddt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tengriddtfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tengridcqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tengridcqtfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tenmrocellgrid", TextOutputFormat.class, NullWritable.class, Text.class);

		// new mr stat
		MultipleOutputs.addNamedOutput(job, MrOutGrid.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrBuild.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrInGrid.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrBuildCell.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, MrInGridCell.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, MrStatCell.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, MrOutGridCell.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, MrOutGridCellNc.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, MrInGridCellNc.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, MrBuildCellNc.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);
		MultipleOutputs.addNamedOutput(job, TopicCellIsolated.TypeName, TextOutputFormat.class, NullWritable.class,
				Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		String tarPath = "";
		if(hdfsOper != null)
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
