package mro.lablefillex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import ImeiCapbility.ImeiConfig;
import StructData.DT_Sample_4G;
import StructData.GridItemOfSize;
import StructData.LteScPlrQciData;
import StructData.NC_LTE;
import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import cellconfig.CellBuildInfo;
import cellconfig.CellBuildWifi;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.GisFunction;
import jan.util.LOGHelper;
import locuser.UserProp;
import locuser_v2.UserLocer;
import mdtstat.MdtNewTableStat;
import mdtstat.UserMdtStat;
import mro.evt.EventData;
import mro.evt.MrErrorEventData;
import mro.lablefill.CellTimeKey;
import mro.lablefill.StatDeal;
import mro.lablefill.StatDeal_CQT;
import mro.lablefill.StatDeal_DT;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStat.UserActTime;
import mro.lablefill.UserActStat.UserCell;
import mro.lablefill.UserActStat.UserCellAll;
import mro.lablefill.UserActStatMng;
import mro.lablefill.XdrLable;
import mro.lablefill.XdrLableMng;
import mro.lablefillex_uemro.FigureFixedOutput;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import mrstat.MroNewTableStat;
import mrstat.TypeInfo;
import mrstat.TypeInfoMng;
import mrstat.TypeResult;
import mrstat.UserMrStat;
import specialUser.SpecialUserConfig;
import util.Func;
import util.MrLocation;
import xdr.lablefill.ResultHelper;
import xdr.locallex.LocItem;

public class MroLableFileReducers
{
	protected static Logger LOG = Logger.getLogger(MroLableFileReducers.class);

	public static class MroDataFileReducers extends DataDealReducer<CellTimeKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private String path_sample;
		private String path_event;
		private String path_cell;
		private String path_cell_freq;
		private String path_cellgrid;
		private String path_grid;
		private String path_ImsiSampleIndex;
		private String path_ImsiEventIndex;
		private String path_myLog;
		private String path_locMore;
		private String path_mroMore;
		private String path_grid_dt;
		private String path_grid_dt_freq;
		private String path_grid_cqt;
		private String path_grid_cqt_freq;
		private String path_sample_dt;
		private String path_sample_dtex;
		private String path_sample_cqt;
		private String path_sample_index_dt;
		private String path_sample_index_cqt;
		private String path_useract_cell;
		private String path_ten_grid;
		private String path_ten_grid_dt;
		private String path_ten_grid_dt_freq;
		private String path_ten_grid_cqt;
		private String path_ten_grid_cqt_freq;
		private String path_ten_cellgrid;
		public String path_freq_lt_cell_byImei;
		public String path_ten_freq_lt_dtGrid_byImei;
		public String path_ten_freq_lt_cqtGrid_byImei;
		public String path_freq_dx_cell_byImei;
		public String path_ten_freq_dx_dtGrid_byImei;
		public String path_ten_freq_dx_cqtGrid_byImei;
		public static String path_cellgrid_dt_10;
		public static String path_cellGrid_cqt_10;
		public static String path_locLib;
		public static String path_xdr_locLib;
		public static String path_special_user_sample;
		public static String path_indoor_err_table;
		public String path_mr_err_evt;

		private String outpath_table;
		private String dateStr;
		private UserMrStat userStat;
		private TypeResult typeResult;

		// mdt
		private UserMdtStat userMdtStat;
		private TypeResult mdtTypeResult;
		private Context context;
		private long tempEci;// 记录上一个eci
		private CellBuildInfo cellBuild;
		private CellBuildWifi cellBuildWifi;
		protected static final Log LOG = LogFactory.getLog(MroDataFileReducers.class);
		private final int TimeSpan = 600;// 10分钟间隔
		private StringBuilder tmSb = new StringBuilder();

		private StatDeal statDeal;
		private StatDeal_DT statDeal_DT;
		private StatDeal_CQT statDeal_CQT;

		private XdrLableMng xdrLableMng;
		private UserActStatMng userActStatMng;
		private static TypeInfoMng typeInfoMng;
		private static TypeInfoMng mdtTypeInfoMng;
		private UserProp userProp;
		private UserLocer userLocer;
		private FigureFixedOutput figureMroFix;// 指纹库定位结果输出
		private long currEci = 0;

		private int IndoorGridSize = 0;
		private int OutdoorGridSize = 0;
		LteCellInfo cellInfo;
		private long count;

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;
		HashMap<String, ArrayList<StructData.MroOrigDataMT>> map = new HashMap<String, ArrayList<StructData.MroOrigDataMT>>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			figureMroFix = new FigureFixedOutput(context, conf);
			figureMroFix.setup();
			IndoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());
			OutdoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getOutDoorSize());

			outpath_table = conf.get("mapreduce.job.oupath");
			String tempData = conf.get("mapreduce.job.date");
			if (tempData != null)
			{
				dateStr = tempData.replace("01_", "");
			}
			path_sample = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample");
			path_event = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_event");
			path_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell");
			path_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell_freq");
			path_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid");
			path_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid");
			path_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiSampleIndex");
			path_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiEventIndex");
			path_myLog = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_myLog");
			path_locMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locMore");
			path_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mroMore");

			path_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt");
			path_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt_freq");
			path_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt");
			path_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt_freq");
			path_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dt");
			path_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dtex");
			path_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_cqt");
			path_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_dt");
			path_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_cqt");

			path_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_useract_cell");

			path_ten_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid");
			path_ten_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt");
			path_ten_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt_freq");
			path_ten_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt");
			path_ten_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt_freq");
			path_ten_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_cellgrid");

			path_freq_lt_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_freq_lt_cell_byImei");
			path_ten_freq_lt_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_dtGrid_byImei");
			path_ten_freq_lt_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_cqtGrid_byImei");

			path_freq_dx_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_freq_dx_cell_byImei");
			path_ten_freq_dx_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_dtGrid_byImei");
			path_ten_freq_dx_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_cqtGrid_byImei");

			path_cellgrid_dt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid_dt_10");
			path_cellGrid_cqt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellGrid_cqt_10");

			path_locLib = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locLib");
			path_xdr_locLib = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_xdr_loclib");
			path_special_user_sample = conf.get("mastercom.mroxdrmerge.mro.special.sample");
			path_indoor_err_table = conf.get("mastercom.mroxdrmerge.path_indoor_err_table");
//20171101 add mr 故障事件
			path_mr_err_evt = conf.get("mastercom.mroxdrmerge.path_mr_err_evt"); 
			
			this.context = context;

			// 初始化输出控制
			if (path_sample.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

			mosMng.SetOutputPath("mrosample", path_sample);
			mosMng.SetOutputPath("mroevent", path_event);
			mosMng.SetOutputPath("mrocell", path_cell);
			mosMng.SetOutputPath("mrocellfreq", path_cell_freq);
			mosMng.SetOutputPath("mrocellgrid", path_cellgrid);
			mosMng.SetOutputPath("mrogrid", path_grid);
			mosMng.SetOutputPath("imsisampleindex", path_ImsiSampleIndex);
			mosMng.SetOutputPath("imsieventindex", path_ImsiEventIndex);
			mosMng.SetOutputPath("myLog", path_myLog);
			mosMng.SetOutputPath("locMore", path_locMore);
			mosMng.SetOutputPath("mroMore", path_mroMore);

			mosMng.SetOutputPath("griddt", path_grid_dt);
			mosMng.SetOutputPath("griddtfreq", path_grid_dt_freq);
			mosMng.SetOutputPath("gridcqt", path_grid_cqt);
			mosMng.SetOutputPath("gridcqtfreq", path_grid_cqt_freq);
			mosMng.SetOutputPath("sampledt", path_sample_dt);
			mosMng.SetOutputPath("sampledtex", path_sample_dtex);
			mosMng.SetOutputPath("samplecqt", path_sample_cqt);
			mosMng.SetOutputPath("sampleindexdt", path_sample_index_dt);
			mosMng.SetOutputPath("sampleindexcqt", path_sample_index_cqt);

			mosMng.SetOutputPath("useractcell", path_useract_cell);

			mosMng.SetOutputPath("tenmrogrid", path_ten_grid);
			mosMng.SetOutputPath("tengriddt", path_ten_grid_dt);
			mosMng.SetOutputPath("tengriddtfreq", path_ten_grid_dt_freq);
			mosMng.SetOutputPath("tengridcqt", path_ten_grid_cqt);
			mosMng.SetOutputPath("tengridcqtfreq", path_ten_grid_cqt_freq);
			mosMng.SetOutputPath("tenmrocellgrid", path_ten_cellgrid);

			mosMng.SetOutputPath("LTfreqcellByImei", path_freq_lt_cell_byImei);
			mosMng.SetOutputPath("LTtenFreqByImeiDt", path_ten_freq_lt_dtGrid_byImei);
			mosMng.SetOutputPath("LTtenFreqByImeiCqt", path_ten_freq_lt_cqtGrid_byImei);

			mosMng.SetOutputPath("DXfreqcellByImei", path_freq_dx_cell_byImei);
			mosMng.SetOutputPath("DXtenFreqByImeiDt", path_ten_freq_dx_dtGrid_byImei);
			mosMng.SetOutputPath("DXtenFreqByImeiCqt", path_ten_freq_dx_cqtGrid_byImei);

			mosMng.SetOutputPath("tencellgriddt", path_cellgrid_dt_10);
			mosMng.SetOutputPath("tencellgridcqt", path_cellGrid_cqt_10);
			mosMng.SetOutputPath("loclib", path_locLib);
			mosMng.SetOutputPath("xdrloclib", path_xdr_locLib);

			mosMng.SetOutputPath("mrvap", path_special_user_sample);
			mosMng.SetOutputPath("indoorErr", path_indoor_err_table);
			
			mosMng.SetOutputPath("mroErrEvt", path_mr_err_evt);

			// fgottstat output
			typeInfoMng = new TypeInfoMng();
			MroNewTableStat.getOutPutPackage(mosMng, typeInfoMng, outpath_table, dateStr);

			// mdt output
			mdtTypeInfoMng = new TypeInfoMng();
			MdtNewTableStat.getOutPutPackage(mosMng, mdtTypeInfoMng, outpath_table, dateStr);

			mosMng.init();
			////////////////////

			// 初始化小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
				throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
			}
			// 初始化imei表
			if (!ImeiConfig.GetInstance().loadImeiCapbility(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "imeiconfig  init error 请检查！");
			}

			// 加载特例用户
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{
				if (!SpecialUserConfig.GetInstance().loadSpecialuser(conf, true))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "specialUser init error 请检查！");
				}
			}

			////////////////////

			statDeal = new StatDeal(mosMng);
			statDeal_DT = new StatDeal_DT(mosMng);
			statDeal_CQT = new StatDeal_CQT(mosMng);
			xdrLableMng = new XdrLableMng();
			userActStatMng = new UserActStatMng();

			// fgottstat
			typeResult = new TypeResult(typeInfoMng);
			userStat = new UserMrStat(typeResult);

			// mdt new table
			mdtTypeResult = new TypeResult(mdtTypeInfoMng);
			userMdtStat = new UserMdtStat(mdtTypeResult);

			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			// 打印状态日志
			LOGHelper.GetLogger().writeLog(LogType.info, "cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			try
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "begin	 cleanup:");
				outUserData();
				outAllData();
				figureMroFix.cleanup();
				LOGHelper.GetLogger().writeLog(LogType.info, "end cleanup:");
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
			}

			super.cleanup(context);

			mosMng.close();
		}

		@Override
		public void reduce(CellTimeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.getEci() != tempEci)
			{
				cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
				if (cellInfo == null)// cell统计需要全量，不能抛弃
				{
					cellInfo = new LteCellInfo();
					LOGHelper.GetLogger().writeLog(LogType.info,
							"gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:" + key.getEci() + "  enbid:" + key.getEci() / 256 + " cellid:" + key.getEci() % 256);
				}
				// 初始化小区楼宇表
				cellBuild = new CellBuildInfo();
				if (!cellBuild.loadCellBuild(conf, (int) key.getEci(), cellInfo.cityid))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellbuild  init error 请检查！eci:" + key.getEci() + " map.size:" + cellBuild.getCellBuildMap().size());
				}
				// 初始化小区楼宇wifi
				cellBuildWifi = new CellBuildWifi();
				if (!cellBuildWifi.loadBuildWifi(conf, (int) key.getEci(), cellInfo.cityid))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellbuildwifi  init error 请检查！eci:" + key.getEci() + " map.size:" + cellBuildWifi.getCellBuildWifiMap().size());
				}
				tempEci = key.getEci();
			}
			if (key.getDataType() == 1)
			{
				// xdrdata
				xdrLableMng = new XdrLableMng();
				for (Text value : values)
				{
					String[] strs = value.toString().split("\t", -1);
					for (int i = 0; i < strs.length; ++i)
					{
						strs[i] = strs[i].trim();
					}
					XdrLable xdrLable;
					try
					{
						xdrLable = XdrLable.FillData(strs, 0);
						if ((xdrLable.longitudeGL > 0 || xdrLable.ilongtude > 0) && (xdrLable.itime / TimeSpan * TimeSpan == key.getTimeSpan())
								&& MainModel.GetInstance().getCompile().Assert(CompileMark.LocAll))
						{
							curText.set(outPutLocLib(xdrLable));
							mosMng.write("xdrloclib", NullWritable.get(), curText);// 吐出xdr位置库
						}
						xdrLableMng.addXdrLocItem(xdrLable);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "XdrLable.FillData error ", e);
						continue;
					}
				}
				xdrLableMng.init();
			}
			else if (key.getDataType() == 2 || key.getDataType() == 5)
			{
				// ott定位
				List<SIGNAL_MR_All> allMroItemList = null;
				if (key.getDataType() == 2)
				{
					allMroItemList = MroLocStat.formatedOttFixed(values, xdrLableMng, cellInfo);
				}
				else if (key.getDataType() == 5)
				{
					allMroItemList = new ArrayList<SIGNAL_MR_All>();
					LOGHelper.GetLogger().writeLog(LogType.info, "begin mergeMro:" + key.getEci());
					MroLocStat.mergeMro(map, values, dataAdapterReader, parseItem);

					List<ArrayList<StructData.MroOrigDataMT>> listMr = new ArrayList<ArrayList<StructData.MroOrigDataMT>>();
					listMr.addAll(map.values());
					map.clear();
					LOGHelper.GetLogger().writeLog(LogType.info, "begin collectData:" + listMr.size());

					for (ArrayList<StructData.MroOrigDataMT> mroList : listMr)
					{
						SIGNAL_MR_All mroItem = MroLocStat.collectData(mroList);
						if (mroItem == null)
						{
							continue;
						}
						MroLocStat.srcMroOttFixed(allMroItemList, mroItem, xdrLableMng, cellInfo);
					}
					LOGHelper.GetLogger().writeLog(LogType.info, "finish collectData:" + listMr.size());
				}
				getInOrOut(allMroItemList);
				// 指纹库 定位代码
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc) || MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc2))
				{
					// 注意mr的分割符，修改配置文件,
					// #FROMAT_MARK:UserLoc
					// #COMPILE_MARK:Home,UserLoc
					// FigureConfigPath SimuLocConfigPath
					// hadoop jar aaaloc.jar NULL 01_170104 NULL NULL NULL
					// MROLOC_NEW
					LOGHelper.GetLogger().writeLog(LogType.info, "userloc  begin ... ...");
					if (currEci == 0 || currEci != key.getEci())
					{
						currEci = key.getEci();
						if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc))
						{
							userProp = new UserProp(conf);
						}
						else
						{
							userLocer = new UserLocer();
						}
					}
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc))
					{
						MroLocStat.UserLocFixed(userProp, allMroItemList, key.getEci());
					}
					else
					{
						MroLocStat.UserLoc2Fixed(userLocer, allMroItemList, mosMng);
					}
					figureMroFix.FigureMroItemList = allMroItemList;
					figureMroFix.outDealingData();
				}
				LOGHelper.GetLogger().writeLog(LogType.info, "begin outDealingData:" + allMroItemList.size());
				// xdr定位输出
				outDealingData(allMroItemList);
				// 吐出位置库
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.LocAll))
				{
					outPutLocLib(allMroItemList);
				}
				LOGHelper.GetLogger().writeLog(LogType.info, "finish outDealingData:" + allMroItemList.size());
			}
			else if (key.getDataType() == 3 || key.getDataType() == 4)
			{
				if (cellInfo == null)// cell统计需要全量，不能抛弃
				{
					cellInfo = new LteCellInfo();
					LOGHelper.GetLogger().writeLog(LogType.info,
							"gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:" + key.getEci() + "  enbid:" + key.getEci() / 256 + " cellid:" + key.getEci() % 256);
				}
				ParseItem parseItem_IMM = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-IMM");
				if (parseItem_IMM == null)
				{
					throw new IOException("parse item do not get.");
				}
				DataAdapterReader dataAdapterReader_IMM = new DataAdapterReader(parseItem_IMM);

				ParseItem parseItem_LOG = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-LOG");
				if (parseItem_LOG == null)
				{
					throw new IOException("parse item do not get.");
				}
				DataAdapterReader dataAdapterReader_LOG = new DataAdapterReader(parseItem_LOG);
				Text value;
				String[] strs = null;
				List<SIGNAL_MR_All> allMdtItemList = new ArrayList<SIGNAL_MR_All>();
				while (values.iterator().hasNext())
				{
					try
					{
						value = values.iterator().next();
						SIGNAL_MR_All mdtItem = new SIGNAL_MR_All();
						try
						{
							if (key.getDataType() == 3)
							{
								strs = value.toString().split(parseItem_IMM.getSplitMark(), -1);
								dataAdapterReader_IMM.readData(strs);
								mdtItem.FillIMMData(dataAdapterReader_IMM);
							}
							else if (key.getDataType() == 4)
							{
								strs = value.toString().split(parseItem_LOG.getSplitMark(), -1);
								dataAdapterReader_LOG.readData(strs);
								mdtItem.FillLOGData(dataAdapterReader_LOG);
							}
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "SIGNAL_MR_All.FillData error ", e);
							continue;
						}

						if (mdtItem == null || mdtItem.tsc == null || mdtItem.tsc.MmeUeS1apId <= 0 || mdtItem.tsc.Eci <= 0 || mdtItem.tsc.beginTime <= 0)
							continue;
						// 附上地市id
						mdtItem.tsc.cityID = cellInfo.cityid;
						xdrLableMng.dealMroData(mdtItem);// 关联imsi
						allMdtItemList.add(mdtItem);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "mro collect err ");
					}
				}
				// 判断in out
				getInOrOut(allMdtItemList);
				// xdr定位输出
				dealSample(allMdtItemList);
			}
		}

		public void getInOrOut(List<SIGNAL_MR_All> mroItemList)
		{
			int buildId = 0;
			for (SIGNAL_MR_All mrAll : mroItemList)
			{
				if (mrAll.tsc.longitude <= 0 || mrAll.tsc.IMSI <= 0)// 没有关联上xdr
				{
					continue;
				}
				if (mrAll.testType != StaticConfig.TestType_CQT)
				{
					mrAll.samState = StaticConfig.ACTTYPE_OUT;
					continue;
				}
				try
				{
					buildId = cellBuild.getBuildId(mrAll.tsc.longitude, mrAll.tsc.latitude);
					if (buildId != 0)
					{
						mrAll.samState = StaticConfig.ACTTYPE_IN;
						mrAll.ispeed = buildId;
						if (cellBuildWifi.getCellBuildWifiMap().size() > 0)
						{
							mrAll.imode = (short) WifiFixed.returnLevel(cellBuildWifi, mrAll.tsc.UserLabel, mrAll.ispeed);
						}
						else
						{
							mrAll.imode = -1;
						}
					}
					else
					{
						mrAll.samState = StaticConfig.ACTTYPE_OUT;
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}

		public void outPutLocLib(List<SIGNAL_MR_All> MrAll_List)
		{
			for (SIGNAL_MR_All item : MrAll_List)
			{
				if (item.tsc.longitude <= 0)
				{
					continue;
				}
				LocItem loclibItem = new LocItem();
				loclibItem.cityID = item.tsc.cityID;
				loclibItem.itime = item.tsc.beginTime;
				loclibItem.wtimems = (short) item.tsc.beginTimems;
				loclibItem.IMSI = item.tsc.IMSI;
				loclibItem.ilongitude = item.tsc.longitude;
				loclibItem.ilatitude = item.tsc.latitude;
				loclibItem.ibuildid = item.ispeed;
				loclibItem.iheight = item.imode;
				loclibItem.testType = item.testType;
				loclibItem.doorType = item.samState;
				loclibItem.radius = item.radius;
				loclibItem.loctp = item.loctp;
				loclibItem.label = item.lable;
				loclibItem.iAreaType = item.areaType;
				loclibItem.iAreaID = item.areaId;
				loclibItem.locSource = item.locSource;
				loclibItem.LteScRSRP = item.tsc.LteScRSRP;
				loclibItem.LteScSinrUL = item.tsc.LteScSinrUL;
				loclibItem.eci = item.tsc.Eci;
				loclibItem.confidentType = item.ConfidenceType;
				loclibItem.msisdn = item.tsc.Msisdn;
				// yzx add 2017.10.24
				loclibItem.s1apid = item.tsc.MmeUeS1apId;
				curText.set(loclibItem.toString());
				try
				{
					mosMng.write("loclib", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "create mr loclib", e);
				}
			}
		}

		/**
		 * 
		 * @param xdrlocation
		 * @return
		 */
		public String outPutLocLib(XdrLable xdrlocation)
		{
			LocItem loclibItem = new LocItem();
			loclibItem.cityID = xdrlocation.cityID;
			loclibItem.itime = xdrlocation.itime;
			loclibItem.wtimems = 0;
			loclibItem.IMSI = xdrlocation.imsi;
			loclibItem.ilongitude = xdrlocation.longitudeGL;
			loclibItem.ilatitude = xdrlocation.latitudeGL;
			loclibItem.testType = xdrlocation.testType;
			if (loclibItem.testType != StaticConfig.TestType_CQT)
			{
				loclibItem.doorType = StaticConfig.ACTTYPE_OUT;
			}
			else
			{
				loclibItem.ibuildid = cellBuild.getBuildId(xdrlocation.longitudeGL, xdrlocation.latitudeGL);
				loclibItem.iheight = WifiFixed.returnLevel(cellBuildWifi, xdrlocation.wifiName, loclibItem.ibuildid);
				if (loclibItem.ibuildid > 0)
				{
					loclibItem.doorType = StaticConfig.ACTTYPE_IN;
				}
				else
				{
					loclibItem.doorType = StaticConfig.ACTTYPE_OUT;
				}
			}
			loclibItem.radius = xdrlocation.radius;
			loclibItem.loctp = xdrlocation.loctp;
			loclibItem.label = xdrlocation.lable;
			loclibItem.iAreaType = xdrlocation.areaType;
			loclibItem.iAreaID = xdrlocation.areaId;
			loclibItem.locSource = Func.getLocSource(xdrlocation.loctp);
			loclibItem.LteScRSRP = 0;
			loclibItem.LteScSinrUL = 0;
			loclibItem.eci = xdrlocation.eci;
			loclibItem.confidentType = Func.getSampleConfidentType(xdrlocation.loctp, loclibItem.doorType, xdrlocation.testType);
			// yzx add 2017.10.24
			loclibItem.s1apid = xdrlocation.s1apid;
			loclibItem.msisdn = xdrlocation.msisdn;
			return loclibItem.toString();
		}

		// 吐出用户过程数据，为了防止内存过多
		private void outDealingData(List<SIGNAL_MR_All> mroItemList)
		{
			dealSample(mroItemList);
			// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
			statDeal.outDealingData();
			statDeal_DT.outDealingData();
			statDeal_CQT.outDealingData();

			// 如果用户数据大于10000个，就吐出去先
			if (userActStatMng.getUserActStatMap().size() > 10000)
			{
				userActStatMng.finalStat();

				// 用户行动信息输出
				StringBuffer sb = new StringBuffer();
				for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
				{
					try
					{
						sb.delete(0, sb.length());

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

		private void dealSample(List<SIGNAL_MR_All> mroList)
		{
			DT_Sample_4G sample = new DT_Sample_4G();
			int dist;
			int maxRadius = 6000;

			for (SIGNAL_MR_All data : mroList)
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
				data.ConfidenceType = Func.getSampleConfidentType(data.locSource, data.samState, data.testType);
				statMro(sample, data);
				if (!sample.mrType.contains("MDT"))// mdt不在参与后面的统计
				{
					statKpi(sample);
				}
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.fgOttStat))
			{
				// mro新表吐出
				userStat.outResult();
				for (Entry<TypeInfo, StringBuffer> entry : typeResult.getMapEntry())
				{
					curText.set(entry.getValue().toString());
					try
					{
						mosMng.write(entry.getKey().getOutPutName(), NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				typeResult.cleanMap();
				// mdt新表吐出
				userMdtStat.outResult();
				for (Entry<TypeInfo, StringBuffer> entry : mdtTypeResult.getMapEntry())
				{
					curText.set(entry.getValue().toString());
					try
					{
						mosMng.write(entry.getKey().getOutPutName(), NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				mdtTypeResult.cleanMap();
			}
		}

		private void statKpi(DT_Sample_4G sample)
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
			if (tTemp.ispeed > 0)
			{
				tsam.ispeed = tTemp.ispeed;
			}
			else
			{
				tsam.ispeed = -1;
			}
			if (tTemp.imode < 0)
			{
				tsam.imode = -1;
			}
			else
			{
				tsam.imode = tTemp.imode;
			}
			tsam.simuLatitude = tTemp.simuLatitude;
			tsam.simuLongitude = tTemp.simuLongitude;
			tsam.testType = tTemp.testType;
			tsam.samState = tTemp.samState;
			tsam.locSource = tTemp.locSource;
			tsam.cityID = tTemp.tsc.cityID;
			tsam.itime = tTemp.tsc.beginTime;
			tsam.wtimems = (short) (tTemp.tsc.beginTimems);
			tsam.ilongitude = tTemp.tsc.longitude;
			tsam.ilatitude = tTemp.tsc.latitude;
			tsam.IMSI = tTemp.tsc.IMSI;
			tsam.UETac = tTemp.UETac;
			tsam.iLAC = (int) MroLocStat.getValidData(tsam.iLAC, tTemp.tsc.TAC);
			tsam.iCI = (long) MroLocStat.getValidData(tsam.iCI, tTemp.tsc.CellId);
			tsam.Eci = (long) MroLocStat.getValidData(tsam.Eci, tTemp.tsc.Eci);
			tsam.eventType = 0;
			tsam.ENBId = (int) MroLocStat.getValidData(tsam.ENBId, tTemp.tsc.ENBId);
			tsam.UserLabel = tTemp.tsc.UserLabel;
			tsam.wifilist = tTemp.tsc.UserLabel;// mrall 中userlabel中装的wifi信息
			tsam.CellId = (long) MroLocStat.getValidData(tsam.CellId, tTemp.tsc.CellId);
			tsam.Earfcn = tTemp.tsc.Earfcn;
			tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
			tsam.MmeCode = (int) MroLocStat.getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
			tsam.MmeGroupId = (int) MroLocStat.getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
			tsam.MmeUeS1apId = (long) MroLocStat.getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
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

			tsam.testType = tTemp.testType;
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
			tsam.fullNetType = ImeiConfig.GetInstance().getValue(tTemp.tsc.imeiTac);
			tsam.eciSwitchList = tTemp.eciSwitchList;
			tsam.ConfidenceType = tTemp.ConfidenceType;
			if (tsam.samState == StaticConfig.ACTTYPE_OUT || tsam.iAreaType > 0)
			{
				tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, OutdoorGridSize);
			}
			else if (tsam.samState == StaticConfig.ACTTYPE_IN)
			{
				tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, IndoorGridSize);
			}
			tsam.MSISDN = tTemp.tsc.Msisdn;

			if (tTemp.tsc.EventType.length() > 0)
			{
				if (tTemp.tsc.EventType.equals("MRO"))
				{
					tsam.flag = "MRO";
				}
				else if (tTemp.tsc.EventType.equals("MDT_IMM"))
				{
					tsam.flag = "MDT_IMM";
				}
				else if (tTemp.tsc.EventType.equals("MDT_LOG"))
				{
					tsam.flag = "MDT_LOG";
				}
				else
				{
					tsam.flag = "MRE";
				}
				tsam.mrType = tTemp.tsc.EventType;
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

			tsam.LteScRSRP_DX = tTemp.LteScRSRP_DX;
			tsam.LteScRSRQ_DX = tTemp.LteScRSRQ_DX;
			tsam.LteScEarfcn_DX = tTemp.LteScEarfcn_DX;
			tsam.LteScPci_DX = tTemp.LteScPci_DX;
			tsam.LteScRSRP_LT = tTemp.LteScRSRP_LT;
			tsam.LteScRSRQ_LT = tTemp.LteScRSRQ_LT;
			tsam.LteScEarfcn_LT = tTemp.LteScEarfcn_LT;
			tsam.LteScPci_LT = tTemp.LteScPci_LT;
			tsam.iAreaID = tTemp.areaId;
			tsam.iAreaType = tTemp.areaType;
			// mdt 置信度
			tsam.Confidence = tTemp.Confidence;

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng) && tTemp.tgsmall.length > 0)
			{
				for (int i = 0; i < tTemp.tgsmall.length; i++)
				{
					tsam.tgsmall[i] = tTemp.tgsmall[i];
				}
			}

			//20171030 add QCI stat
			tsam.qciData = new LteScPlrQciData(tTemp.tsc.LteScPlrULQci, tTemp.tsc.LteScPlrDLQci);
			
			calJamType(tsam);
			// output to hbase
			try
			{
				// 特殊用户吐出全量的sample
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) && SpecialUserConfig.GetInstance().ifSpeciUser(tsam.IMSI, false))
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("mrvap", NullWritable.get(), curText);
				}

				// 新表计算
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.fgOttStat))
				{
					if (tsam.mrType.equals("MDT_IMM") || tsam.mrType.equals("MDT_LOG"))
					{
						userMdtStat.dealSample(tsam);
						return;
					}
					else
					{
						userStat.dealSample(tsam);
					}
				}

				if (tTemp.loctp.contains("fp"))// 过滤掉指纹库定位,常住小区回填的loctp=fp
				{
					tsam.ilongitude = 0;
					tsam.ilatitude = 0;
					return;
				}

				if (MainModel.GetInstance().getCompile().Assert(CompileMark.OutAllSample))
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("mrosample", NullWritable.get(), curText);
				}
				
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.MroDetail))
				{
					List<EventData> evtDatas = tsam.toEventData();
					StringBuffer bf = new StringBuffer();
					for(EventData evtData : evtDatas)
					{	
						if(evtData instanceof MrErrorEventData){
							evtData.toString(bf);
							curText.set(bf.toString());
							mosMng.write("mroErrEvt", NullWritable.get(), curText);
							bf.delete(0, bf.length());
						}
					}
				}
				
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
	}
}
