package mro.lablefillex;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cellconfig.FilterCellConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mdtstat.MdtNewTableStat;
import mro.format_mt.MroFormatMapperSiChuan.MroMapper_ERICSSON;
import mro.format_mt.MroFormatMapperSiChuan.MroMapper_HUAWEI_TD;
import mro.format_mt.MroFormatMapperSiChuan.MroMapper_NSN_TD;
import mro.format_mt.MroFormatMapperSiChuan.MroMapper_ZTE_TD;
import mro.lablefill.CellTimeKey;
import mro.lablefill.MroLableMapper_FMT.MroDataMapper_FMT;
import mro.lablefillex.MdtDataMapper.MdtImmMapper;
import mro.lablefillex.MdtDataMapper.MdtLogMapper;
import mro.lablefillex.MroLableFileReducers.MroDataFileReducers;
import mro.lablefillex.MroLableMappers.CellPartitioner;
import mro.lablefillex.MroLableMappers.CellSortKeyComparator;
import mro.lablefillex.MroLableMappers.CellSortKeyGroupComparator;
import mro.lablefillex.MroLableMappers.MroDataMapperByEciTime;
import mro.lablefillex.MroLableMappers.MroDataMappers;
import mro.lablefillex.MroLableMappers.XdrLocationMappers;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import mrstat.MroNewTableStat;
import util.HdfsHelper;
import util.MaprConfHelper;
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

public class MroLableFillMains
{
	protected static final Log LOG = LogFactory.getLog(MroLableFillMains.class);
	private static int reduceNum;
	public static String queueName;
	public static String inpath_xdr;
	public static String inpath_mre;
	public static String inpath_mro;
	public static String inpath_mdtimm;
	public static String inpath_mdtlog;
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
	// 新添加 区分dt /cqt 2017-05-23
	public static String path_cellgrid_dt_10;
	public static String path_cellGrid_cqt_10;

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

	// 新添加 区分dt /cqt 2017-05-23
	public static String fpath_cellgrid_dt_10;
	public static String fpath_cellGrid_cqt_10;
	// 新版本MR统计
	public static String path_mr_outgrid;
	public static String path_mr_build;
	public static String path_mr_ingrid;
	public static String path_mr_build_cell;
	public static String path_mr_ingrid_cell;
	public static String path_mr_cell;
	public static String path_mr_outgrid_cell;
	// public static String path_mr_CELL_NC;
	public static String path_mr_outgrid_CELL_NC;
	public static String path_mr_ingrid_CELL_NC;
	public static String path_mr_build_CELL_NC;
	public static String path_topic_cell_isolated;
	// ling shi tian jia shu chu
	public static String path_ppp;
	// new freq cell/grid 170518
	public static String path_freq_lt_cell_byImei;
	public static String path_ten_freq_lt_dtGrid_byImei;
	public static String path_ten_freq_lt_cqtGrid_byImei;

	public static String fpath_freq_lt_cell_byImei;
	public static String fpath_ten_freq_lt_dtGrid_byImei;
	public static String fpath_ten_freq_lt_cqtGrid_byImei;

	public static String path_freq_dx_cell_byImei;
	public static String path_ten_freq_dx_dtGrid_byImei;
	public static String path_ten_freq_dx_cqtGrid_byImei;
	public static String fpath_freq_dx_cell_byImei;
	public static String fpath_ten_freq_dx_dtGrid_byImei;
	public static String fpath_ten_freq_dx_cqtGrid_byImei;
	// 生成位置库存放路径
	public static String path_locLib;
	public static String path_special_user_sample;
	public static String path_xdr_loclib;
	// 分析室分隐性故障
	public static String path_indoor_err_table;
	// mr故障事件（单通/断续）
	public static String path_mr_err_evt;

	private static void makeConfig_home(Configuration conf, String[] args)
	{// NULL NULL 01_170103 NULL d:/data/mro d:/data/out NULL

		reduceNum = 10;
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
		if (args.length >= 10)
		{
			inpath_mdtlog = args[8];
			inpath_mdtimm = args[9];
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
		// 新添加 区分dt /cqt 2017-05-23
		path_cellgrid_dt_10 = outpath_table + "/TB_DTSIGNAL_CELLGRID10_" + outpath_date;
		path_cellGrid_cqt_10 = outpath_table + "/TB_CQTSIGNAL_CELLGRID10_" + outpath_date;

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

		// 新添加 区分dt /cqt 2017-05-23
		fpath_cellgrid_dt_10 = outpath_table + "/TB_FGDTSIGNAL_CELLGRID10_" + outpath_date;
		fpath_cellGrid_cqt_10 = outpath_table + "/TB_FGCQTSIGNAL_CELLGRID10_" + outpath_date;

		// new mr stat
		path_mr_outgrid = outpath_table + "/TB_MR_OUTGRID_" + outpath_date.substring(3);
		path_mr_build = outpath_table + "/TB_MR_BUILD_" + outpath_date.substring(3);
		path_mr_ingrid = outpath_table + "/TB_MR_INGRID_" + outpath_date.substring(3);
		path_mr_build_cell = outpath_table + "/TB_MR_BUILD_CELL_" + outpath_date.substring(3);
		path_mr_ingrid_cell = outpath_table + "/TB_MR_INGRID_CELL_" + outpath_date.substring(3);
		path_mr_cell = outpath_table + "/TB_STAT_MR_CELL_" + outpath_date.substring(3);
		path_mr_outgrid_cell = outpath_table + "/TB_MR_OUTGRID_CELL_" + outpath_date.substring(3);
		path_mr_outgrid_CELL_NC = outpath_table + "/TB_MR_OUTGRID_CELL_NC_" + outpath_date.substring(3);
		path_mr_ingrid_CELL_NC = outpath_table + "/TB_MR_INGRID_CELL_NC_" + outpath_date.substring(3);
		path_mr_build_CELL_NC = outpath_table + "/TB_MR_BUILD_CELL_NC_" + outpath_date.substring(3);
		path_topic_cell_isolated = outpath_table + "/TB_TOPIC_CELL_ISOLATED_" + outpath_date;
		path_ppp = outpath_table + "/TB_PPP_" + outpath_date;
		// new freq cell/grid 170518
		path_freq_lt_cell_byImei = outpath_table + "/TB_FREQ_SIGNAL_LT_CELL_BYIMEI_" + outpath_date;
		path_ten_freq_lt_dtGrid_byImei = outpath_table + "/TB_FREQ_DTSIGNAL_LT_GRID10_BYIMEI_" + outpath_date;
		path_ten_freq_lt_cqtGrid_byImei = outpath_table + "/TB_FREQ_CQTSIGNAL_LT_GRID10_BYIMEI_" + outpath_date;

		fpath_freq_lt_cell_byImei = outpath_table + "/TB_FREQ_FGSIGNAL_LT_CELL_BYIMEI_" + outpath_date;
		fpath_ten_freq_lt_dtGrid_byImei = outpath_table + "/TB_FREQ_FGDTSIGNAL_LT_GRID10_BYIMEI_" + outpath_date;
		fpath_ten_freq_lt_cqtGrid_byImei = outpath_table + "/TB_FREQ_FGCQTSIGNAL_LT_GRID10_BYIMEI_" + outpath_date;

		path_freq_dx_cell_byImei = outpath_table + "/TB_FREQ_SIGNAL_DX_CELL_BYIMEI_" + outpath_date;
		path_ten_freq_dx_dtGrid_byImei = outpath_table + "/TB_FREQ_DTSIGNAL_DX_GRID10_BYIMEI_" + outpath_date;
		path_ten_freq_dx_cqtGrid_byImei = outpath_table + "/TB_FREQ_CQTSIGNAL_DX_GRID10_BYIMEI_" + outpath_date;
		fpath_freq_dx_cell_byImei = outpath_table + "/TB_FREQ_FGSIGNAL_DX_CELL_BYIMEI_" + outpath_date;
		fpath_ten_freq_dx_dtGrid_byImei = outpath_table + "/TB_FREQ_FGDTSIGNAL_DX_GRID10_BYIMEI_" + outpath_date;
		fpath_ten_freq_dx_cqtGrid_byImei = outpath_table + "/TB_FREQ_FGCQTSIGNAL_DX_GRID10_BYIMEI_" + outpath_date;

		// 170630
		path_locLib = outpath_table + "/TB_LOC_LIB_" + outpath_date;
		path_xdr_loclib = outpath_table + "/XDR_LOC_LIB_" + outpath_date;
		path_special_user_sample = outpath_table + "/TB_SIGNAL_MR_VAP_" + outpath_date;
		//
		path_indoor_err_table = outpath_table + "/tb_signal_indoor_err_" + outpath_date;
		
		path_mr_err_evt = outpath_table + "/tb_mr_evt_dd_" + outpath_date;
		
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
		LOG.info(path_mr_outgrid_CELL_NC);
		LOG.info(path_mr_ingrid_CELL_NC);
		LOG.info(path_mr_build_CELL_NC);
		LOG.info(path_topic_cell_isolated);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		conf.set("mapreduce.job.date", outpath_date);
		conf.set("mapreduce.job.oupath", outpath_table);

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
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid_CELL_NC", path_mr_outgrid_CELL_NC);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid_CELL_NC", path_mr_ingrid_CELL_NC);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mr_build_CELL_NC", path_mr_build_CELL_NC);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_topic_cell_isolated", path_topic_cell_isolated);
		conf.set("mastercom.mroxdrmerge.ppp", path_ppp);
		// new freq cell、grid byImei
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_freq_lt_cell_byImei", path_freq_lt_cell_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_dtGrid_byImei", path_ten_freq_lt_dtGrid_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_cqtGrid_byImei", path_ten_freq_lt_cqtGrid_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_freq_lt_cell_byImei", fpath_freq_lt_cell_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_lt_dtGrid_byImei", fpath_ten_freq_lt_dtGrid_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_lt_cqtGrid_byImei", fpath_ten_freq_lt_cqtGrid_byImei);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_freq_dx_cell_byImei", path_freq_dx_cell_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_dtGrid_byImei", path_ten_freq_dx_dtGrid_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_cqtGrid_byImei", path_ten_freq_dx_cqtGrid_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_freq_dx_cell_byImei", fpath_freq_dx_cell_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_dx_dtGrid_byImei", fpath_ten_freq_dx_dtGrid_byImei);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_dx_cqtGrid_byImei", fpath_ten_freq_dx_cqtGrid_byImei);

		// 新添加 区分dt /cqt 2017-05-23
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_cellgrid_dt_10", fpath_cellgrid_dt_10);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.fpath_cellGrid_cqt_10", fpath_cellGrid_cqt_10);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid_dt_10", path_cellgrid_dt_10);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cellGrid_cqt_10", path_cellGrid_cqt_10);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_locLib", path_locLib);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_xdr_loclib", path_xdr_loclib);

		conf.set("mastercom.mroxdrmerge.mro.special.sample", path_special_user_sample);
		conf.set("mastercom.mroxdrmerge.path_indoor_err_table", path_indoor_err_table);
		
		conf.set("mastercom.mroxdrmerge.path_mr_err_evt", path_mr_err_evt);

		MaprConfHelper.CustomMaprParas(conf);

		// The minimum size chunk that map input should be split into. Note that
		// some file formats may have minimum split sizes that take priority
		// over this setting.
		// 将小文件进行整合
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

	/**
	 * 
	 * @param hdfsOper
	 * @param path
	 * @param ifMuster
	 *            是不是必须要有该配置
	 * @throws Exception
	 */
	public static void checkBuildP(HDFSOper hdfsOper, String path) throws Exception
	{
		FileStatus[] files = hdfsOper.getFs().listStatus(new Path(path));
		if (files.length < 1)
		{
			throw (new Exception("there is no file: " + path));
		}
		if (files[0].getPath().getName().split("_").length < 5)
		{
			throw (new Exception("please update  " + path));
		}
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		if (args.length < 7)
		{
			System.err.println("Usage: Mro_loc <queueName> ,<outpath_date>,<inpath_xdr> ,<inpath_mro>,<outpath_table> ,<inpath_mre>");
			throw (new Exception("MroLableFillMains args input error!"));
		}

		makeConfig_home(conf, args);

		HDFSOper hdfsOper = null;
		if (!inpath_mro.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			String cellBuildPath = MainModel.GetInstance().getAppConfig().getCellBuildPath();
			String cellBuildWifiPath = MainModel.GetInstance().getAppConfig().getCellBuildPath();
			checkBuildP(hdfsOper, cellBuildPath);
			try
			{
				checkBuildP(hdfsOper, cellBuildWifiPath);// wifi配置不是必须正确
			}
			catch (Exception e)
			{
				System.out.println(e.getMessage());
			}
		}

		String mrFilter = "";
		if (args.length >= 8)
		{
			if (!args[7].toUpperCase().equals("NULL") && !args[7].toUpperCase().equals(""))
			{
				mrFilter = args[7];
			}
		}

		Job job = Job.getInstance(conf, "MroXdrMerge.mro.locfillex" + ":" + outpath_date);
		job.setJarByClass(MroLableFillMains.class);
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

		if (!inpath_xdr.equals("NULL"))
		{
			inpaths = inpath_xdr.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (!tm_inpath.contains(":") && hdfsOper.checkDirExist(tm_inpath))
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
			if (!tm_inpath.contains(":") && hdfsOper.checkDirExist(tm_inpath))
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath, false, mrFilter);
			}
			else
			{
				LOG.info("path not exists : " + tm_inpath);
			}
		}

		if (!inpath_mre.equals("NULL"))
		{
			inpaths = inpath_mre.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false, mrFilter);
				}
				else
				{
					LOG.info("path not exists : " + tm_inpath);
				}
			}
		}

		if (!inpath_mdtimm.equals("NULL"))
		{
			inpaths = inpath_mdtimm.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpath))
				{
					inputSize += hdfsOper.getSizeOfPath(tm_inpath, false, mrFilter);
				}
				else
				{
					LOG.info("path not exists : " + tm_inpath);
				}
			}
		}

		if (!inpath_mdtlog.equals("NULL"))
		{
			inpaths = inpath_mdtlog.split(",", -1);
			for (String tm_inpath : inpaths)
			{
				if (hdfsOper != null && hdfsOper.checkDirExist(tm_inpath))
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
			int dealReduceSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getDealSizeReduce());
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			double sizePerReduce = dealReduceSize / 1024.0;

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.FilterCell) && FilterCellConfig.GetInstance().loadFilterCell(conf)
					&& FilterCellConfig.GetInstance().getLteCellInfoList().size() > 0)
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

		///////////////////////////////////////////////////////

		if (!inpath_xdr.equals("NULL") && (inpath_xdr.contains(":") || hdfsOper.checkDirExist(inpath_xdr)))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_xdr), CombineTextInputFormat.class, XdrLocationMappers.class);
		}
		else
		{
			LOG.info("No xdrlocation data!");
		}

		if (!inpath_mdtlog.equals("NULL") && (inpath_mdtlog.contains(":") || hdfsOper.checkDirExist(inpath_mdtlog)))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mdtlog), CombineTextInputFormat.class, MdtLogMapper.class);
		}
		else
		{
			LOG.info("No MDT_LOG data!");
		}

		if (!inpath_mdtimm.equals("NULL") && (inpath_mdtimm.contains(":") || hdfsOper.checkDirExist(inpath_mdtimm)))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mdtimm), CombineTextInputFormat.class, MdtImmMapper.class);
		}
		else
		{
			LOG.info("No MDT_IMM data!");
		}

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mro), CombineTextInputFormat.class, MroDataMapper_FMT.class);
		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing) || MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi) || MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
		{
			for (String inputMro : inpath_mro.split(",", -1))
			{
				if (inputMro.contains(":") || hdfsOper.checkDirExist(inputMro))
				{
					MultipleInputs.addInputPath(job, new Path(inputMro), CombineTextInputFormat.class, MroDataMapperByEciTime.class);
				}
			}
		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro))
		{
			String[] inputMro = inpath_mro.split(",", -1);
			String ERICSSON_path = inputMro[0];
			String HUAWEI_path = inputMro[1];
			String NSN_path = inputMro[2];
			String ZTE_path = inputMro[3];

			MultipleInputs.addInputPath(job, new Path(ERICSSON_path), CombineTextInputFormat.class, MroMapper_ERICSSON.class);
			MultipleInputs.addInputPath(job, new Path(HUAWEI_path), CombineTextInputFormat.class, MroMapper_HUAWEI_TD.class);
			MultipleInputs.addInputPath(job, new Path(NSN_path), CombineTextInputFormat.class, MroMapper_NSN_TD.class);
			MultipleInputs.addInputPath(job, new Path(ZTE_path), CombineTextInputFormat.class, MroMapper_ZTE_TD.class);
		}
		else if (inpath_mro.contains(":") || hdfsOper.checkDirExist(inpath_mro))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mro), CombineTextInputFormat.class, MroDataMappers.class);
		}
		else
		{
			LOG.info("No mro data!");
		}

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng) && hdfsOper.checkDirExist(inpath_mre))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mre), CombineTextInputFormat.class, MroDataMapper_FMT.class);
		}
		else if ((MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro) || MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu) || MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi))
				&& (hdfsOper != null && !inpath_mre.equals("NULL") && hdfsOper.checkDirExist(inpath_mre)))
		{
			for (String inputMro : inpath_mre.split(",", -1))
			{
				MultipleInputs.addInputPath(job, new Path(inputMro), CombineTextInputFormat.class, MroDataMapperByEciTime.class);
			}
		}
		else if (hdfsOper != null && !inpath_mre.equals("NULL") && hdfsOper.checkDirExist(inpath_mre))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_mre), CombineTextInputFormat.class, MroDataMappers.class);
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
		MultipleOutputs.addNamedOutput(job, MrBuildCell.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrInGridCell.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrStatCell.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrOutGridCell.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrOutGridCellNc.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrInGridCellNc.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MrBuildCellNc.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, TopicCellIsolated.TypeName, TextOutputFormat.class, NullWritable.class, Text.class);
		// new freq cell/grid
		MultipleOutputs.addNamedOutput(job, "LTfreqcellByImei", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "LTtenFreqByImeiDt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "LTtenFreqByImeiCqt", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "DXfreqcellByImei", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "DXtenFreqByImeiDt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "DXtenFreqByImeiCqt", TextOutputFormat.class, NullWritable.class, Text.class);
		// 新添加dt/cqt cellGrid 170523
		MultipleOutputs.addNamedOutput(job, "tencellgriddt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tencellgridcqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrvap", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "ppp", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "loclib", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xdrloclib", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "indoorErr", TextOutputFormat.class, NullWritable.class, Text.class);
		
		MultipleOutputs.addNamedOutput(job, "mroErrEvt", TextOutputFormat.class, NullWritable.class, Text.class);

		// ott_fg_stat
		MroNewTableStat.registerOutFileName(job);
		// mdt 新表
		MdtNewTableStat.registerOutFileName(job);

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
