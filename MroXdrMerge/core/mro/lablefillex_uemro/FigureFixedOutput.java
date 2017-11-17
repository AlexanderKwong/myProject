package mro.lablefillex_uemro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import StructData.DT_Sample_4G;
import StructData.NC_LTE;
import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.GisFunction;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStatMng;
import mro.lablefill_xdr_figure.MroLableFileReducers.MroDataFileReducers;
import mro.lablefill.UserActStat.UserActTime;
import mro.lablefill.UserActStat.UserCell;
import mro.lablefill.UserActStat.UserCellAll;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.MrLocation;
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
import xdr.lablefill.ResultHelper;
import xdr.lablefill.TopicCellIsolated;

public class FigureFixedOutput
{
	public List<SIGNAL_MR_All> FigureMroItemList = new ArrayList<SIGNAL_MR_All>();
	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText = new Text();
	public Context context;
	public Configuration conf;

	/////////////////////// 指纹库定位输出路径///////////////////
	private String fpath_sample;
	private String fpath_event;
	private String fpath_cell;
	private String fpath_cell_freq;
	private String fpath_cellgrid;
	private String fpath_grid;
	private String fpath_ImsiSampleIndex;
	private String fpath_ImsiEventIndex;
	private String fpath_myLog;
	private String fpath_locMore;
	private String fpath_mroMore;

	private String fpath_grid_dt;
	private String fpath_grid_dt_freq;
	private String fpath_grid_cqt;
	private String fpath_grid_cqt_freq;
	private String fpath_sample_dt;
	private String fpath_sample_dtex;
	private String fpath_sample_cqt;
	private String fpath_sample_index_dt;
	private String fpath_sample_index_cqt;

	private String fpath_ten_grid;
	private String fpath_ten_grid_dt;
	private String fpath_ten_grid_dt_freq;
	private String fpath_ten_grid_cqt;
	private String fpath_ten_grid_cqt_freq;
	private String fpath_ten_cellgrid;
	private String fpath_useract_cell;

	// 新版本MR统计
	public String path_mr_outgrid;
	public String path_mr_build;
	public String path_mr_ingrid;
	public String path_mr_build_cell;
	public String path_mr_ingrid_cell;
	public String path_mr_cell;
	public String path_mr_outgrid_cell;
	public String path_mr_outgrid_CELL_NC;
	public String path_mr_ingrid_CELL_NC;
	public String path_mr_build_CELL_NC;
	public String path_topic_cell_isolated;

	// 临时添加
	// freq cell /grid by Imei
	public String fpath_freq_lt_cell_byImei;
	public String fpath_ten_freq_lt_dtGrid_byImei;
	public String fpath_ten_freq_lt_cqtGrid_byImei;

	public static String fpath_freq_dx_cell_byImei;
	public static String fpath_ten_freq_dx_dtGrid_byImei;
	public static String fpath_ten_freq_dx_cqtGrid_byImei;

	// dt cqt cellGrid 170523
	public static String fpath_cellgrid_dt_10;
	public static String fpath_cellGrid_cqt_10;

	protected final Log LOG = LogFactory.getLog(MroDataFileReducers.class);

	private StringBuilder tmSb = new StringBuilder();

	private StatDeal statDeal;
	private StatDeal_DT statDeal_DT;
	private StatDeal_CQT statDeal_CQT;
	private UserActStatMng userActStatMng;

	public FigureFixedOutput(Context context, Configuration conf)
	{
		this.context = context;
		this.conf = conf;
	}

	public void setup() throws IOException, InterruptedException
	{
		MainModel.GetInstance().setConf(conf);

		////////////////////// 初始化输出控制指纹库定位
		fpath_sample = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample");
		fpath_event = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_event");
		fpath_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cell");
		fpath_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cell_freq");
		fpath_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cellgrid");
		fpath_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid");
		fpath_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ImsiSampleIndex");
		fpath_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ImsiEventIndex");
		fpath_myLog = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_myLog");
		fpath_locMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_locMore");
		fpath_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_mroMore");

		fpath_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_dt");
		fpath_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_dt_freq");
		fpath_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_cqt");
		fpath_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_cqt_freq");
		fpath_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_dt");
		fpath_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_dtex");
		fpath_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_cqt");
		fpath_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_index_dt");
		fpath_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_index_cqt");

		fpath_ten_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid");
		fpath_ten_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_dt");
		fpath_ten_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_dt_freq");
		fpath_ten_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_cqt");
		fpath_ten_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_cqt_freq");
		fpath_ten_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_cellgrid");

		fpath_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_useract_cell");

		// new mr stat
		path_mr_outgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid");
		path_mr_build = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_build");
		path_mr_ingrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid");
		path_mr_build_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_build_cell");
		path_mr_ingrid_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid_cell");
		path_mr_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_cell");
		path_mr_outgrid_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid_cell");
		path_mr_outgrid_CELL_NC = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_outgrid_CELL_NC");
		path_mr_ingrid_CELL_NC = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_ingrid_CELL_NC");
		path_mr_build_CELL_NC = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mr_build_CELL_NC");
		path_topic_cell_isolated = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_topic_cell_isolated");
		// freq cell/grid byImei
		fpath_freq_lt_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_freq_lt_cell_byImei");
		fpath_ten_freq_lt_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_lt_dtGrid_byImei");
		fpath_ten_freq_lt_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_lt_cqtGrid_byImei");

		fpath_freq_dx_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_freq_dx_cell_byImei");
		fpath_ten_freq_dx_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_dx_dtGrid_byImei");
		fpath_ten_freq_dx_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_freq_dx_cqtGrid_byImei");
		// dt cqt cellGrid 170523
		fpath_cellgrid_dt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cellgrid_dt_10");
		fpath_cellGrid_cqt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cellGrid_cqt_10");

		// 初始化输出控制
		if (fpath_sample.contains(":"))
			mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
		else
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

		////////////////////// 初始化输出控制指纹库定位

		mosMng.SetOutputPath("mrosample", fpath_sample);
		mosMng.SetOutputPath("mroevent", fpath_event);
		mosMng.SetOutputPath("mrocell", fpath_cell);
		mosMng.SetOutputPath("mrocellfreq", fpath_cell_freq);
		mosMng.SetOutputPath("mrocellgrid", fpath_cellgrid);
		mosMng.SetOutputPath("mrogrid", fpath_grid);
		mosMng.SetOutputPath("imsisampleindex", fpath_ImsiSampleIndex);
		mosMng.SetOutputPath("imsieventindex", fpath_ImsiEventIndex);
		mosMng.SetOutputPath("myLog", fpath_myLog);
		mosMng.SetOutputPath("locMore", fpath_locMore);
		mosMng.SetOutputPath("mroMore", fpath_mroMore);
		mosMng.SetOutputPath("griddt", fpath_grid_dt);
		mosMng.SetOutputPath("griddtfreq", fpath_grid_dt_freq);
		mosMng.SetOutputPath("gridcqt", fpath_grid_cqt);
		mosMng.SetOutputPath("gridcqtfreq", fpath_grid_cqt_freq);
		mosMng.SetOutputPath("sampledt", fpath_sample_dt);
		mosMng.SetOutputPath("sampledtex", fpath_sample_dtex);
		mosMng.SetOutputPath("samplecqt", fpath_sample_cqt);
		mosMng.SetOutputPath("sampleindexdt", fpath_sample_index_dt);
		mosMng.SetOutputPath("sampleindexcqt", fpath_sample_index_cqt);
		mosMng.SetOutputPath("useractcell", fpath_useract_cell);

		mosMng.SetOutputPath("tenmrogrid", fpath_ten_grid);
		mosMng.SetOutputPath("tengriddt", fpath_ten_grid_dt);
		mosMng.SetOutputPath("tengriddtfreq", fpath_ten_grid_dt_freq);
		mosMng.SetOutputPath("tengridcqt", fpath_ten_grid_cqt);
		mosMng.SetOutputPath("tengridcqtfreq", fpath_ten_grid_cqt_freq);
		mosMng.SetOutputPath("tenmrocellgrid", fpath_ten_cellgrid);

		// 新MR统计
		mosMng.SetOutputPath(MrOutGrid.TypeName, path_mr_outgrid);
		mosMng.SetOutputPath(MrBuild.TypeName, path_mr_build);
		mosMng.SetOutputPath(MrInGrid.TypeName, path_mr_ingrid);
		mosMng.SetOutputPath(MrBuildCell.TypeName, path_mr_build_cell);
		mosMng.SetOutputPath(MrInGridCell.TypeName, path_mr_ingrid_cell);
		mosMng.SetOutputPath(MrStatCell.TypeName, path_mr_cell);
		mosMng.SetOutputPath(MrOutGridCell.TypeName, path_mr_outgrid_cell);
		mosMng.SetOutputPath(MrOutGridCellNc.TypeName, path_mr_outgrid_CELL_NC);
		mosMng.SetOutputPath(MrInGridCellNc.TypeName, path_mr_ingrid_CELL_NC);
		mosMng.SetOutputPath(MrBuildCellNc.TypeName, path_mr_build_CELL_NC);
		mosMng.SetOutputPath(TopicCellIsolated.TypeName, path_topic_cell_isolated);
		// freq cell/grid byImei
		mosMng.SetOutputPath("LTfreqcellByImei", fpath_freq_lt_cell_byImei);
		mosMng.SetOutputPath("LTtenFreqByImeiDt", fpath_ten_freq_lt_dtGrid_byImei);
		mosMng.SetOutputPath("LTtenFreqByImeiCqt", fpath_ten_freq_lt_cqtGrid_byImei);

		mosMng.SetOutputPath("DXfreqcellByImei", fpath_freq_dx_cell_byImei);
		mosMng.SetOutputPath("DXtenFreqByImeiDt", fpath_ten_freq_dx_dtGrid_byImei);
		mosMng.SetOutputPath("DXtenFreqByImeiCqt", fpath_ten_freq_dx_cqtGrid_byImei);

		// dt cqt cellGrid 170523
		mosMng.SetOutputPath("tencellgriddt", fpath_cellgrid_dt_10);
		mosMng.SetOutputPath("tencellgridcqt", fpath_cellGrid_cqt_10);

		mosMng.init();

		// 初始化小区的信息
		if (!CellConfig.GetInstance().loadLteCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
			throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
		}

		statDeal = new StatDeal(mosMng);
		statDeal_DT = new StatDeal_DT(mosMng);
		statDeal_CQT = new StatDeal_CQT(mosMng);
		userActStatMng = new UserActStatMng();

		// 打印状态日志
		LOGHelper.GetLogger().writeLog(LogType.info, "cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
	}

	public void cleanup() throws IOException, InterruptedException
	{
		try
		{
			outUserData();
			outAllData();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
		}
		mosMng.close();
	}

	// 吐出用户过程数据，为了防止内存过多
	public void outDealingData()
	{
		dealSample();

		// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
		statDeal.outDealingData();
		statDeal_DT.outDealingData();
		statDeal_CQT.outDealingData();

		// 如果用户数据大于10000个，就吐出去先
		if (userActStatMng.getUserActStatMap().size() > 10000)
		{
			userActStatMng.finalStat();

			// 用户行动信息输出
			for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
			{
				try
				{
					StringBuffer sb = new StringBuffer();
					String TabMark = "\t";
					for (UserActTime userActTime : userActStat.userActTimeMap.values())
					{
						for (UserCellAll userActAll : userActTime.userCellAllMap.values())
						{
							sb.delete(0, sb.length());

							sb.append(0);// cityid
							sb.append(TabMark);
							sb.append(userActStat.imsi);
							sb.append(TabMark);
							sb.append(userActStat.msisdn);
							sb.append(TabMark);
							sb.append(userActTime.stime);
							sb.append(TabMark);
							sb.append(userActTime.etime);
							sb.append(TabMark);

							// 主服小区
							UserCell mainUserCell = userActAll.getMainUserCell();
							sb.append(userActAll.eci);
							sb.append(TabMark);
							sb.append(0);
							sb.append(TabMark);
							sb.append(userActAll.eci);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpSum);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpTotal);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpMaxMark);
							sb.append(TabMark);
							sb.append(mainUserCell.rsrpMinMark);

							curText.set(sb.toString());
							mosMng.write("useractcell", NullWritable.get(), curText);

							// 邻区
							List<UserCell> userCellList = userActAll.getUserCellList();
							int sn = 1;
							for (UserCell userCell : userCellList)
							{
								if (userCell.eci == userActAll.eci)
								{
									continue;
								}

								sb.delete(0, sb.length());
								sb.append(0);// cityid
								sb.append(TabMark);
								sb.append(userActStat.imsi);
								sb.append(TabMark);
								sb.append(userActStat.msisdn);
								sb.append(TabMark);
								sb.append(userActTime.stime);
								sb.append(TabMark);
								sb.append(userActTime.etime);
								sb.append(TabMark);

								sb.append(userActAll.eci);
								sb.append(TabMark);
								sb.append(sn);
								sb.append(TabMark);
								sb.append(userCell.eci);
								sb.append(TabMark);
								sb.append(userCell.rsrpSum);
								sb.append(TabMark);
								sb.append(userCell.rsrpTotal);
								sb.append(TabMark);
								sb.append(userCell.rsrpMaxMark);
								sb.append(TabMark);
								sb.append(userCell.rsrpMinMark);
								curText.set(sb.toString());
								mosMng.write("useractcell", NullWritable.get(), curText);
								sn++;
							}
						}
					}
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "user action error", e);
				}
			}
			userActStatMng = new UserActStatMng();
		}

		// for (ppp temp : pppList)
		// {
		// curText.set(temp.toLine());
		// try
		// {
		// mosMng.write("ppp", NullWritable.get(), curText);
		// }
		// catch (Exception e)
		// {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
	}

	// 将会吐出用户最后所有数据
	private void outUserData()
	{

	}

	private void outAllData()
	{
		statDeal.outAllData();
		statDeal_DT.outAllData();
		statDeal_CQT.outAllData();

		userActStatMng.finalStat();
		// 用户行动信息输出
		for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
		{
			try
			{
				StringBuffer sb = new StringBuffer();
				String TabMark = "\t";
				for (UserActTime userActTime : userActStat.userActTimeMap.values())
				{
					for (UserCellAll userActAll : userActTime.userCellAllMap.values())
					{
						sb.delete(0, sb.length());

						sb.append(0);// cityid
						sb.append(TabMark);
						sb.append(userActStat.imsi);
						sb.append(TabMark);
						sb.append(userActStat.msisdn);
						sb.append(TabMark);
						sb.append(userActTime.stime);
						sb.append(TabMark);
						sb.append(userActTime.etime);
						sb.append(TabMark);

						// 主服小区
						UserCell mainUserCell = userActAll.getMainUserCell();
						sb.append(userActAll.eci);
						sb.append(TabMark);
						sb.append(0);
						sb.append(TabMark);
						sb.append(userActAll.eci);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpSum);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpTotal);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpMaxMark);
						sb.append(TabMark);
						sb.append(mainUserCell.rsrpMinMark);

						curText.set(sb.toString());
						mosMng.write("useractcell", NullWritable.get(), curText);

						// 邻区
						List<UserCell> userCellList = userActAll.getUserCellList();
						int sn = 1;
						for (UserCell userCell : userCellList)
						{
							if (userCell.eci == userActAll.eci)
							{
								continue;
							}

							sb.delete(0, sb.length());
							sb.append(0);// cityid
							sb.append(TabMark);
							sb.append(userActStat.imsi);
							sb.append(TabMark);
							sb.append(userActStat.msisdn);
							sb.append(TabMark);
							sb.append(userActTime.stime);
							sb.append(TabMark);
							sb.append(userActTime.etime);
							sb.append(TabMark);

							sb.append(userActAll.eci);
							sb.append(TabMark);
							sb.append(sn);
							sb.append(TabMark);
							sb.append(userCell.eci);
							sb.append(TabMark);
							sb.append(userCell.rsrpSum);
							sb.append(TabMark);
							sb.append(userCell.rsrpTotal);
							sb.append(TabMark);
							sb.append(userCell.rsrpMaxMark);
							sb.append(TabMark);
							sb.append(userCell.rsrpMinMark);

							curText.set(sb.toString());
							mosMng.write("useractcell", NullWritable.get(), curText);
							sn++;
						}

					}
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "user action error", e);
			}
		}

	}

	private void dealSample()
	{
		DT_Sample_4G sample = new DT_Sample_4G();
		int dist;
		int maxRadius = 6000;

		for (SIGNAL_MR_All data : FigureMroItemList)
		{

			sample.Clear();

			// 如果采样点过远就需要筛除
			LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(data.tsc.Eci);
			dist = -1;
			if (lteCellInfo != null)
			{
				if (data.tsc.longitude > 0 && data.tsc.latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
				{
					dist = (int) GisFunction.GetDistance(data.tsc.longitude, data.tsc.latitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
				}
			}
			data.dist = dist;
			if (dist > maxRadius)
			{
				data.dist = -1;
				data.tsc.longitude = 0;
				data.tsc.latitude = 0;
				data.testType = StaticConfig.TestType_OTHER;
			}

			// 基于Ta进行筛
			if (data.tsc.LteScTadv >= 15 && data.tsc.LteScTadv < 1282)
			{
				double taDist = MrLocation.calcDist(data.tsc.LteScTadv, data.tsc.LteScRTTD);
				if (dist > taDist * 1.2)
				{
					data.dist = -1;
					data.tsc.longitude = 0;
					data.tsc.latitude = 0;
					data.testType = StaticConfig.TestType_OTHER;
				}
			}

			statMro(sample, data);
			statKpi(sample);
		}
	}

	public void statKpi(DT_Sample_4G sample)
	{
		// cpe不参与kpi运算
		if (sample.testType == StaticConfig.TestType_CPE)
		{
			return;
		}

		statDeal.dealSample(sample);
		userActStatMng.stat(sample);

		// StaticConfig.TestType_DT_EX 不参与运算
		if (sample.testType == StaticConfig.TestType_DT)
		{
			statDeal_DT.dealSample(sample);
		}

		if (sample.testType == StaticConfig.TestType_CQT)
		{
			statDeal_CQT.dealSample(sample);
		}

	}

	private void statMro(DT_Sample_4G tsam, SIGNAL_MR_All tTemp)
	{
		tsam.ispeed = tTemp.ispeed; // 标识经纬度来源
		tsam.imode = tTemp.imode;
		tsam.simuLatitude = tTemp.simuLatitude;
		tsam.simuLongitude = tTemp.simuLongitude;
		tsam.testType = tTemp.testType;
		tsam.locSource = tTemp.locSource;
		tsam.cityID = tTemp.tsc.cityID;
		tsam.itime = tTemp.tsc.beginTime;
		tsam.wtimems = (short) (tTemp.tsc.beginTimems);
		tsam.ilongitude = tTemp.tsc.longitude;
		tsam.ilatitude = tTemp.tsc.latitude;
		tsam.IMSI = tTemp.tsc.IMSI;
		tsam.UETac = tTemp.UETac;
		tsam.iLAC = (int) getValidData(tsam.iLAC, tTemp.tsc.TAC);
		tsam.iCI = (long) getValidData(tsam.iCI, tTemp.tsc.CellId);
		tsam.Eci = (long) getValidData(tsam.Eci, tTemp.tsc.Eci);

		tsam.eventType = 0;
		tsam.ENBId = (int) getValidData(tsam.ENBId, tTemp.tsc.ENBId);
		tsam.UserLabel = tTemp.tsc.UserLabel;
		tsam.CellId = (long) getValidData(tsam.CellId, tTemp.tsc.CellId);
		tsam.Earfcn = tTemp.tsc.Earfcn;
		tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
		tsam.MmeCode = (int) getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
		tsam.MmeGroupId = (int) getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
		tsam.MmeUeS1apId = (long) getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
		tsam.Weight = tTemp.tsc.Weight;
		tsam.LteScRSRP = tTemp.tsc.LteScRSRP;
		tsam.LteScRSRQ = tTemp.tsc.LteScRSRQ;
		tsam.LteScEarfcn = tTemp.tsc.LteScEarfcn;
		tsam.LteScPci = tTemp.tsc.LteScPci;
		tsam.LteScBSR = tTemp.tsc.LteScBSR;
		tsam.LteScRTTD = tTemp.tsc.LteScRTTD;
		tsam.LteScTadv = tTemp.tsc.LteScTadv;
		tsam.LteScAOA = tTemp.tsc.LteScAOA;
		tsam.LteScPHR = tTemp.tsc.LteScPHR;
		tsam.LteScRIP = tTemp.tsc.LteScRIP;
		tsam.LteScSinrUL = tTemp.tsc.LteScSinrUL;
		tsam.LocFillType = 1;

		if (tTemp.testType == 0)
		{
			tsam.testType = StaticConfig.TestType_DT_EX;
		}
		else
		{
			tsam.testType = tTemp.testType;
		}

		tsam.location = tTemp.location;
		tsam.dist = tTemp.dist;
		tsam.radius = tTemp.radius;
		tsam.loctp = tTemp.loctp;
		tsam.indoor = tTemp.indoor;
		tsam.networktype = tTemp.networktype;
		tsam.lable = tTemp.lable;

		tsam.serviceType = tTemp.serviceType;
		tsam.serviceSubType = tTemp.subServiceType;

		tsam.moveDirect = tTemp.moveDirect;

		tsam.LteScPUSCHPRBNum = tTemp.tsc.LteScPUSCHPRBNum;
		tsam.LteScPDSCHPRBNum = tTemp.tsc.LteScPDSCHPRBNum;
		tsam.imeiTac = tTemp.tsc.imeiTac;

		if (tTemp.tsc.EventType.length() > 0)
		{
			if (tTemp.tsc.EventType.equals("MRO"))
			{
				tsam.flag = "MRO";
			}
			else
			{
				tsam.flag = "MRE";
				tsam.mrType = tTemp.tsc.EventType;
			}
		}
		else
		{
			tsam.flag = "MRO";
			int mrTypeIndex = tTemp.tsc.UserLabel.indexOf(",");
			if (mrTypeIndex >= 0)
			{
				tsam.flag = "MRE";
				tsam.mrType = tTemp.tsc.UserLabel.substring(mrTypeIndex + 1);
			}
		}

		for (int i = 0; i < tsam.nccount.length; i++)
		{
			tsam.nccount[i] = tTemp.nccount[i];
		}

		for (int i = 0; i < tsam.tlte.length; i++)
		{
			tsam.tlte[i] = tTemp.tlte[i];
		}

		for (int i = 0; i < tsam.ttds.length; i++)
		{
			tsam.ttds[i] = tTemp.ttds[i];
		}

		for (int i = 0; i < tsam.tgsm.length; i++)
		{
			tsam.tgsm[i] = tTemp.tgsm[i];
		}

		for (int i = 0; i < tsam.trip.length; i++)
		{
			tsam.trip[i] = tTemp.trip[i];
		}

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng) && tTemp.tgsmall.length > 0)
		{
			for (int i = 0; i < tTemp.tgsmall.length; i++)
			{
				tsam.tgsmall[i] = tTemp.tgsmall[i];
			}
		}

		calJamType(tsam);

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoOutSample))
		{
			return;
		}

		try
		{
			if (tsam.testType == StaticConfig.TestType_DT)
			{
				// if (tsam.ilongitude > 0)
				// {
				curText.set(ResultHelper.getPutLteSample(tsam));
				mosMng.write("sampledt", NullWritable.get(), curText);
				// }
			}
			else if (tsam.testType == StaticConfig.TestType_DT_EX || tsam.testType == StaticConfig.TestType_CPE)
			{
				// if (tsam.ilongitude > 0)
				// {
				curText.set(ResultHelper.getPutLteSample(tsam));
				mosMng.write("sampledtex", NullWritable.get(), curText);
				// }
			}
			else if (tsam.testType == StaticConfig.TestType_CQT)
			{
				curText.set(ResultHelper.getPutLteSample(tsam));
				mosMng.write("samplecqt", NullWritable.get(), curText);
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
			{
				// 吐出关联的中间结果
				tmSb.delete(0, tmSb.length());
				tmSb.append(tsam.Eci + "_" + tsam.MmeUeS1apId + "_" + tsam.itime);
				tmSb.append("\t");
				tmSb.append(tsam.Earfcn);
				tmSb.append("_");
				tmSb.append(tsam.LteScPci);
				tmSb.append("_");
				tmSb.append(tsam.LteScRSRP);
				tmSb.append("_");
				tmSb.append(tsam.IMSI);
				tmSb.append("_");
				tmSb.append(tsam.ilongitude);
				tmSb.append("_");
				tmSb.append(tsam.ilatitude);

				curText.set(tmSb.toString());
				mosMng.write("mroMore", NullWritable.get(), curText);
			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "output event error ", e);
			// TODO: handle exception
		}

	}

	public void statMro(DT_Sample_4G tsam)
	{
		try
		{
			curText.set(ResultHelper.getPutLteSample(tsam));
			mosMng.write("mrosample", NullWritable.get(), curText);

			if (tsam.testType == StaticConfig.TestType_DT)
			{
				if (tsam.ilongitude > 0)
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("sampledt", NullWritable.get(), curText);
				}
			}
			else if (tsam.testType == StaticConfig.TestType_DT_EX || tsam.testType == StaticConfig.TestType_CPE)
			{
				if (tsam.ilongitude > 0)
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("sampledtex", NullWritable.get(), curText);
				}
			}
			else if (tsam.testType == StaticConfig.TestType_CQT)
			{
				curText.set(ResultHelper.getPutLteSample(tsam));
				mosMng.write("samplecqt", NullWritable.get(), curText);
			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "output event error ", e);
			// TODO: handle exception
		}
	}

	public void calJamType(DT_Sample_4G tsam)
	{
		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						tsam.sfcnJamCellCount++;
					}
					else
					{
						tsam.dfcnJamCellCount++;
					}
				}
			}
		}
	}

	private Object getValidData(Object srcData, Object tarData)
	{
		if (tarData instanceof Integer)
		{
			if ((Integer) tarData != 0 && (Integer) tarData != StaticConfig.Int_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		else if (tarData instanceof Long)
		{
			if ((Long) tarData != 0 && (Long) tarData != StaticConfig.Long_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		return srcData;
	}

	public int getValidValueInt(int srcValue, int targValue)
	{
		if (targValue != StaticConfig.Int_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public String getValidValueString(String srcValue, String targValue)
	{
		if (!targValue.equals(""))
		{
			return targValue;
		}
		return srcValue;
	}

	public long getValidValueLong(long srcValue, long targValue)
	{
		if (targValue != StaticConfig.Long_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

}
