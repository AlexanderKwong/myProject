package xdr.lablefill;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import com.google.protobuf.TextFormat.ParseException;
import StructData.DT_Event;
import StructData.DT_Sample_23G;
import StructData.DT_Sample_4G;
import StructData.ELocationMark;
import StructData.ELocationType;
import StructData.SIGNAL_LOC;
import StructData.SIGNAL_XDR_23G;
import StructData.SIGNAL_XDR_4G;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.GsmCellInfo;
import cellconfig.LteCellInfo;
import cellconfig.TdCellInfo;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.GisFunction;
import jan.util.GisLocater;
import jan.util.GisPos;
import jan.util.LOGHelper;
import jan.util.data.MyInt;
import mdtstat.Util;
import mro.lablefill.XdrLable;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import specialUser.SpecialUserConfig;
import util.Func;
import xdr.lablefill.UserActStat.UserAct;
import xdr.lablefill.UserActStat.UserActTime;
import xdr.lablefill.UserInfoMng.UserInfo;
import xdr.lablefill.by23g.LocInfoItem;
import xdr.rail.highspeed.HiRailConfig;
import xdr.rail.highspeed.RailFillFunc;

public class XdrLableFileSeqReducer
{
	private static DecimalFormat doubleFormat = new DecimalFormat("#.00");

	public static class XdrDataFileReducer extends DataDealReducer<ImsiTimeKey, Text, NullWritable, Text>
	{
		protected static final Log LOG = LogFactory.getLog(XdrDataFileReducer.class);

		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private String path_sample;
		private String path_event;
		private String path_event_mme;
		private String path_event_http;
		private String path_cell;
		private String path_cellgrid;
		private String path_cellgrid_23g;
		private String path_grid;
		private String path_grid_hour;
		private String path_grid_user_hour;
		private String path_grid_23g;
		private String path_ImsiSampleIndex;
		private String path_ImsiEventIndex;
		private String path_xdrLoc;
		private String path_xdrMore;
		private String path_xdrLocation;
		private String path_grid_dt;
		private String path_grid_dt_23g;
		private String path_grid_cqt;
		private String path_grid_cqt_23g;
		private String path_cellgrid_cqt_23g;
		private String path_event_dt;
		private String path_event_dt_23g;
		private String path_event_dtex;
		private String path_event_dtex_23g;
		private String path_event_cqt;
		private String path_event_cqt_23g;
		private String path_event_index_dt;
		private String path_event_index_cqt;
		private String path_userinfo;
		private String path_useract;
		private String path_event_err;
		private String path_event_err_23g;
		private Context context;
		private final int TimeSpan = 600;// 10分钟间隔
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private StatDeal statDeal;
		private StatDeal_DT statDeal_DT;
		private StatDeal_CQT statDeal_CQT;
		private long curImsi;
		private LableItemMng lableItemMng;
		private LocationItemMng locationItemMng;
		private LocationWFItemMng locationWFItemMng;
		private LocationImsiCellLocMng locationImsiCellLocMng;// 用户常住小区
		private LabelDeal labelDeal;
		private UserInfoMng userInfoMng;
		private List<SIGNAL_LOC> userXdrList;
		private boolean bOutAllUserData;
		private int totalXdrCount;
		private int formatErrCount;
		private int eciErrCount;
		private int locErrCount;
		private int radiusErrCount;
		private int lteXdrLocCount;
		private int locCount_wf;
		private int locCount_ll;
		private int locCount_cl;
		private int xdrlocCount_lll;
		private int locCount_lll;
		private int eciErrCount_23g;
		private int locErrCount_23g;
		private int radiusErrCount_23g;
		private String userCellHourTimePath;
		private String path_loc_lib;
		private String path_special_user_event;
		private String path_xdrHiRailPath;

		private Map<Long, MyInt> cellLocDic = new HashMap<Long, MyInt>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);

			path_sample = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_sample");
			path_event = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event");
			path_event_http = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_http");
			path_event_mme = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_mme");

			path_cell = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cell");
			path_cellgrid = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid");
			path_cellgrid_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_23g");
			path_grid = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid");
			path_grid_hour = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_hour");
			path_grid_user_hour = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_user_hour");
			path_grid_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_23g");
			path_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_ImsiSampleIndex");
			path_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_ImsiEventIndex");
			path_xdrLoc = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_xdrLoc");
			path_xdrMore = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_xdrMore");
			path_xdrLocation = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_xdrLocation");

			path_grid_dt = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt");
			path_grid_dt_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt_23g");
			path_grid_cqt = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt");
			path_grid_cqt_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt_23g");
			path_cellgrid_cqt_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_cqt_23g");
			path_event_dt = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dt");
			path_event_dt_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dt_23g");
			path_event_dtex = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex");
			path_event_dtex_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex_23g");
			path_event_cqt = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt");
			path_event_cqt_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt_23g");
			path_event_index_dt = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_index_dt");
			path_event_index_cqt = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_index_cqt");

			path_userinfo = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_userinfo");
			path_useract = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_useract");
			path_event_err = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_err");
			path_event_err_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_err_23g");
			userCellHourTimePath = conf.get("mastercom.mroxdrmerge.xdr.locfill.userCellHourTimePath");
			path_loc_lib = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_loc_lib");
			path_special_user_event = conf.get("mastercom.mroxdrmerge.evt.vap");
			path_xdrHiRailPath = conf.get("mastercom.mroxdrmerge.path_xdrHiRailPath");

			this.context = context;

			if (!path_event_http.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");

			mosMng.SetOutputPath("xdrsample", path_sample);
			mosMng.SetOutputPath("xdrevent", path_event);
			mosMng.SetOutputPath("xdrmmeevent", path_event_mme);
			mosMng.SetOutputPath("xdrhttpevent", path_event_http);
			mosMng.SetOutputPath("xdrcell", path_cell);
			mosMng.SetOutputPath("xdrcellgrid", path_cellgrid);
			mosMng.SetOutputPath("xdrcellgrid23g", path_cellgrid_23g);
			mosMng.SetOutputPath("xdrgrid", path_grid);
			mosMng.SetOutputPath("xdrgridhour", path_grid_hour);
			mosMng.SetOutputPath("xdrgriduserhour", path_grid_user_hour);
			mosMng.SetOutputPath("xdrgrid23g", path_grid_23g);
			mosMng.SetOutputPath("imsisampleindex", path_ImsiSampleIndex);
			mosMng.SetOutputPath("imsieventindex", path_ImsiEventIndex);
			mosMng.SetOutputPath("xdrloc", path_xdrLoc);
			mosMng.SetOutputPath("xdrMore", path_xdrMore);
			mosMng.SetOutputPath("xdrLocation", path_xdrLocation);

			mosMng.SetOutputPath("griddt", path_grid_dt);
			mosMng.SetOutputPath("griddt23g", path_grid_dt_23g);
			mosMng.SetOutputPath("gridcqt", path_grid_cqt);
			mosMng.SetOutputPath("gridcqt23g", path_grid_cqt_23g);
			mosMng.SetOutputPath("cellgridcqt23g", path_cellgrid_cqt_23g);
			mosMng.SetOutputPath("eventdt", path_event_dt);
			mosMng.SetOutputPath("eventdt23g", path_event_dt_23g);
			mosMng.SetOutputPath("eventdtex", path_event_dtex);
			mosMng.SetOutputPath("eventdtex23g", path_event_dtex_23g);
			mosMng.SetOutputPath("eventcqt", path_event_cqt);
			mosMng.SetOutputPath("eventcqt23g", path_event_cqt_23g);
			mosMng.SetOutputPath("eventindexdt", path_event_index_dt);
			mosMng.SetOutputPath("eventindexcqt", path_event_index_cqt);

			mosMng.SetOutputPath("userinfo", path_userinfo);
			mosMng.SetOutputPath("useract", path_useract);
			mosMng.SetOutputPath("eventerr", path_event_err);
			mosMng.SetOutputPath("eventerr23g", path_event_err_23g);
			mosMng.SetOutputPath("cellhourTime", userCellHourTimePath);
			mosMng.SetOutputPath("loclib", path_loc_lib);
			mosMng.SetOutputPath("evtVap", path_special_user_event);
			mosMng.SetOutputPath("hirailxdr", path_xdrHiRailPath);

			mosMng.init();
			////////////////////

			LOGHelper.GetLogger().writeLog(LogType.info, "hdfs url is: " + MainModel.GetInstance().getFsUrl());
			// 初始化lte小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！" + CellConfig.GetInstance().errLog);
				throw (new IOException("ltecell init error 请检查！"));
			}

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail))// 高铁需要的配置
			{
				if (!HiRailConfig.loadConfig(conf))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "HiRail  init error 请检查！" + HiRailConfig.errLog.toString());
				}
			}

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
			{
				if (!SpecialUserConfig.GetInstance().loadSpecialuser(conf, true))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "specialUser init error 请检查！");
				}
			}

			if (!path_event_http.contains(":"))
			{
				// 初始化gsm小区的信息
				if (!CellConfig.GetInstance().loadGsmCell(conf))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "gsmcell init error 请检查！");
					// throw (new IOException("gsmcell init error 请检查！"));
				}

				// 初始化tdscdma小区的信息
				if (!CellConfig.GetInstance().loadTdCell(conf))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "tdcell init error 请检查！");
					// throw (new IOException("tdcell init error 请检查！"));
				}
			}

			////////////////////

			curImsi = 0;
			eciErrCount = 0;
			locErrCount = 0;
			radiusErrCount = 0;
			totalXdrCount = 0;
			formatErrCount = 0;
			lteXdrLocCount = 0;
			locCount_wf = 0;
			locCount_ll = 0;
			locCount_cl = 0;

			userXdrList = new ArrayList<SIGNAL_LOC>();

			statDeal = new StatDeal(mosMng);
			statDeal.cellLocDic = cellLocDic;
			statDeal_DT = new StatDeal_DT(mosMng);
			statDeal_DT.cellLocDic = cellLocDic;
			statDeal_CQT = new StatDeal_CQT(mosMng);
			statDeal_CQT.cellLocDic = cellLocDic;

			lableItemMng = new LableItemMng();
			locationItemMng = new LocationItemMng();
			locationWFItemMng = new LocationWFItemMng();
			userInfoMng = new UserInfoMng();
			locationImsiCellLocMng = new LocationImsiCellLocMng();

			bOutAllUserData = false;

			// 打印状态日志
			LOGHelper.GetLogger().writeLog(LogType.info, "ltecell init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
			LOGHelper.GetLogger().writeLog(LogType.info, "gsmcell init count is : " + CellConfig.GetInstance().getGsmCellInfoMap().size());
			LOGHelper.GetLogger().writeLog(LogType.info, "tdcell init count is : " + CellConfig.GetInstance().getTDCellInfoMap().size());

		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "total xdr count is :" + totalXdrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "format error count is :" + formatErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "error eci count is :" + eciErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "error location count is :" + locErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "error radius count is :" + radiusErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "lte xdr location count is :" + lteXdrLocCount);

			LOGHelper.GetLogger().writeLog(LogType.info, "gps location count is :" + locCount_lll);
			LOGHelper.GetLogger().writeLog(LogType.info, "lte xdr with gps location count is :" + xdrlocCount_lll);

			LOGHelper.GetLogger().writeLog(LogType.info, "23G eci error location count is :" + eciErrCount_23g);
			LOGHelper.GetLogger().writeLog(LogType.info, "23G location error count is :" + locErrCount_23g);
			LOGHelper.GetLogger().writeLog(LogType.info, "23G radius error count is :" + radiusErrCount_23g);

			super.cleanup(context);

			outAllData();
			mosMng.close();
		}

		public void reduce(ImsiTimeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException, ParseException
		{
			try
			{
				if (curImsi != key.getImsi())
				{
					LOGHelper.GetLogger().writeLog(LogType.info, "user xdr count info : " + curImsi + " " + userXdrList.size());

					outUserData();

					userXdrList = new ArrayList<SIGNAL_LOC>();
					lableItemMng = new LableItemMng();
					locationItemMng = new LocationItemMng();
					locationWFItemMng = new LocationWFItemMng();
					locationImsiCellLocMng = new LocationImsiCellLocMng();
					curImsi = key.getImsi();
					labelDeal = new LabelDeal(curImsi, mosMng);
				}

				// 用户常驻小区列表
				if (key.getDataType() == 0)
				{
					locationImsiCellLocMng.setImsi(key.getImsi());
					for (Text value : values)
					{
						String[] strs = value.toString().split(",", -1);
						LocationImsiCellTime temp = new LocationImsiCellTime(strs);
						if (temp.longtitude > 0)
						{
							locationImsiCellLocMng.putItem(temp);
						}
					}
				}
				// 经纬度信息 location信息
				if (key.getDataType() == 1)
				{
					locationItemMng = new LocationItemMng();
					for (Text value : values)
					{
						String[] strs = value.toString().split("\\|" + "|" + "\t", -1);

						try
						{
							LocationItem item = new LocationItem();
							if (strs[0].equals("URI") || strs[0].equals(""))
							{
								item.FillData(strs, 1);
							}
							else
							{
								item.FillData(strs, 0);
							}
							locationItemMng.AddItem(item);
							locCount_lll++;
						}
						catch (Exception e)
						{

						}

						LOGHelper.GetLogger().writeLog(LogType.info, value.toString());
					}
					locationItemMng.finInit();
				}
				else if (key.getDataType() == 2) // lable 信息
				{
					lableItemMng = new LableItemMng();
					for (Text value : values)
					{
						String[] strs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\t", -1);

						LableItem xdrLocItem = new LableItem();
						if (xdrLocItem.FillData(strs, 0))
						{
							lableItemMng.AddItem(xdrLocItem);
						}
					}
					lableItemMng.init();
				}
				else if (key.getDataType() == 3)
				{
					locationWFItemMng = new LocationWFItemMng();
					for (Text value : values)
					{
						String[] strs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\t", -1);

						LocationWFItem item = new LocationWFItem();
						if (item.FillData(strs, 0))
						{
							locationWFItemMng.AddItem(item);
						}
					}
					locationWFItemMng.finInit();
				}
				else if (key.getDataType() == 11)// cpe用户处理
				{
					List<SIGNAL_LOC> xdrItemList = new ArrayList<SIGNAL_LOC>();
					int curTimeSpan = 0;
					Text value;

					ParseItem parseItem_MME = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
					if (parseItem_MME == null)
					{
						throw new IOException("parse item do not get.");
					}
					DataAdapterReader dataAdapterReader_MME = new DataAdapterReader(parseItem_MME);

					ParseItem parseItem_HTTP = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
					if (parseItem_HTTP == null)
					{
						throw new IOException("parse item do not get.");
					}
					DataAdapterReader dataAdapterReader_HTTP = new DataAdapterReader(parseItem_HTTP);

					ParseItem parseItem_HX = MainModel.GetInstance().getDataAdapterConfig().getParseItem("HX-XDR");
					if (parseItem_HX == null)
					{
						throw new IOException("parse item do not get.");
					}
					DataAdapterReader dataAdapterReader_HX = new DataAdapterReader(parseItem_HX);

					while (values.iterator().hasNext())
					{
						totalXdrCount++;

						value = values.iterator().next();
						String tmStr = value.toString();
						String strDataType = tmStr.substring(0, tmStr.indexOf("#"));
						String strs = tmStr.substring(tmStr.indexOf("#") + 1);

						SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
						int dataType = Integer.parseInt(strDataType);
						try
						{
							if (dataType == XdrLableMapper.DATATYPE_4G_MME)
							{
								dataAdapterReader_MME.readData(strs);
								if (!xdrItem.FillData_SEQ_MME(dataAdapterReader_MME))
								{
									continue;
								}
							}
							else if (dataType == XdrLableMapper.DATATYPE_4G_HTTP)
							{
								dataAdapterReader_HTTP.readData(strs);
								if (!xdrItem.FillData_SEQ_HTTP(dataAdapterReader_HTTP))
								{
									continue;
								}
							}
							else if (dataType == XdrLableMapper.DATATYPE_23G)
							{

							}
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "xdrItem.FillData error : " + value.toString(), e);
							formatErrCount++;
							continue;
						}

						// 保留原始数据
						xdrItem.valStr = new String(value.toString());

						// 统计算是有经纬度的点个数
						if (xdrItem.Eci > 0 && xdrItem.longitude > 0)
						{
							lteXdrLocCount++;

							MyInt myTemp = cellLocDic.get(xdrItem.Eci);
							if (myTemp == null)
							{
								myTemp = new MyInt(0);
								cellLocDic.put(xdrItem.Eci, myTemp);
							}
							myTemp.data++;
						}

						// dd司机判断
						if (xdrItem.location == 3)
						{
							labelDeal.setDDDriver(true);
							// LOGHelper.GetLogger().writeLog(LogType.info,
							// "find dd driver user : " + value.toString());
						}

						// =============================采样点有效性判断=============================================

						if (xdrItem.IMSI <= 0 || xdrItem.Eci <= 0 || xdrItem.stime <= 0)
						{
							if (xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "数据内容有问题，忽略");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
							}

							continue;
						}

						LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(xdrItem.Eci);
						int maxRadius = 6000;
						if (lteCellInfo != null)
						{
							if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
							{
								xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
							}

							maxRadius = Math.min(maxRadius, 5 * lteCellInfo.radius);

							xdrItem.cityID = lteCellInfo.cityid;
						}

						if (xdrItem.cityID < 0)
						{
							if (xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "eci关联配置失败");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
							}

							eciErrCount++;
							continue;
						}

						if (xdrItem.dist >= maxRadius && xdrItem.longitude > 0)
						{
							curText.set(xdrItem.valStr + "\t" + "小区距离采样点过远");
							if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
								mosMng.write("eventerr", NullWritable.get(), curText);
							locErrCount++;
							xdrItem.cleanLoc();
						}

						if (xdrItem.dist < 0 && xdrItem.longitude > 0)
						{
							curText.set(xdrItem.valStr + "\t" + "小区经纬度配置缺失");
							if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
								mosMng.write("eventerr", NullWritable.get(), curText);
							locErrCount++;
							continue;
						}

						// 如果是location > 6 属于不可预知的情况,去掉
						if (xdrItem.location > 6)
						{
							if (xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "location不在识别范围内");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
							}

							continue;
						}

						if (xdrItem.longitude > 0)
						{

							// 如果location 是 0，1 那么将经纬度抹掉
							if (xdrItem.location >= 0 && xdrItem.location <= 1)
							{
								curText.set(xdrItem.valStr + "\t" + "location为01不予分析，忽略");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
							}
							else if (xdrItem.location == ELocationMark.BaiDuSDK.getValue())
							{
								if (xdrItem.loctp.length() == 0)
								{
									curText.set(xdrItem.valStr + "\t" + "百度地图的loctp未知");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}
								else if (xdrItem.loctp.equals(ELocationType.Cell.getName()))
								{
									curText.set(xdrItem.valStr + "\t" + "百度地图的小区定位不可用");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}
								else if (xdrItem.radius > 100)
								{
									curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
									radiusErrCount++;
								}
							}
							else if (xdrItem.location == ELocationMark.GaoDeSDK.getValue())
							{
								if (!xdrItem.loctp.equals("14") && !xdrItem.loctp.equals("24") && !xdrItem.loctp.equals("5") && !xdrItem.loctp.equals(ELocationType.Wifi.getName()))
								{
									curText.set(xdrItem.valStr + "\t" + "高德地图的loctp未知");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}
								else if (xdrItem.radius > 100)
								{
									curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
									radiusErrCount++;
								}
							}
							// 腾讯地图loctp需要转换
							else if (xdrItem.location == ELocationMark.TengXunSDK.getValue())
							{
								if (xdrItem.radius > 50)
								{
									// 腾讯定位精度>20的，都没有参考意义
									curText.set(xdrItem.valStr + "\t" + "腾讯地图的定位经度不符合要求，忽略");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}
							}

						}

						// =============================采样点有效性判断=============================================

						if (curTimeSpan == 0)
						{
							curTimeSpan = xdrItem.stime / TimeSpan * TimeSpan;
						}

						xdrItemList.add(xdrItem);

					}

					// 10分钟一包数据，格式化后，就可以吐出
					dealCPEData(xdrItemList);
					xdrItemList.clear();
				}
				else if (key.getDataType() == 10)// 正常xdr数据
				{
					List<SIGNAL_LOC> xdrItemList = new ArrayList<SIGNAL_LOC>();
					int curTimeSpan = 0;
					Text value;

					ParseItem parseItem_MME = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-MME");
					if (parseItem_MME == null)
					{
						throw new IOException("parse item do not get.");
					}
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.xdrFormat) && !MainModel.GetInstance().changedMmePosFlag)
					{
						List<String> colunNameList = new ArrayList<String>(parseItem_MME.getColumPosMap().keySet());
						Collections.sort(colunNameList);// 对字段按照字段名称排序
						for (int i = 0; i < colunNameList.size(); i++)
						{
							parseItem_MME.setPos(colunNameList.get(i), i);
						}
						MainModel.GetInstance().changedMmePosFlag = true;
					}
					DataAdapterReader dataAdapterReader_MME = new DataAdapterReader(parseItem_MME);

					ParseItem parseItem_HTTP = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
					if (parseItem_HTTP == null)
					{
						throw new IOException("parse item do not get.");
					}
					DataAdapterReader dataAdapterReader_HTTP = new DataAdapterReader(parseItem_HTTP);

					ParseItem parseItem_HX = MainModel.GetInstance().getDataAdapterConfig().getParseItem("HX-XDR");
					if (parseItem_HX == null)
					{
						throw new IOException("parse item do not get.");
					}
					DataAdapterReader dataAdapterReader_HX = new DataAdapterReader(parseItem_HX);

					while (values.iterator().hasNext())
					{
						totalXdrCount++;

						value = values.iterator().next();
						String tmStr = value.toString();
						String strDataType = tmStr.substring(0, tmStr.indexOf("#"));
						String strs = tmStr.substring(tmStr.indexOf("#") + 1);
						int dataType = Integer.parseInt(strDataType);

						if (dataType == XdrLableMapper.DATATYPE_4G_MME || dataType == XdrLableMapper.DATATYPE_4G_HTTP)
						{
							SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
							try
							{
								if (dataType == XdrLableMapper.DATATYPE_4G_MME)
								{
									dataAdapterReader_MME.readData(strs);
									if (!xdrItem.FillData_SEQ_MME(dataAdapterReader_MME))
									{
										continue;
									}

									LocationItem locItem = locationItemMng.getLableItem(xdrItem.stime);// 找间隔时间最近的location
									if (locItem != null)
									{
										xdrItem.longitude = locItem.longitude;
										xdrItem.latitude = locItem.latitude;
										xdrItem.location = locItem.location;
										xdrItem.loctp = locItem.loctp;
										xdrItem.radius = locItem.radius;
										xdrItem.latlng_time = locItem.locTime;
										xdrItem.wifiName = locItem.wifiName;
									}
									else
									{
										LocationWFItem locWFItem = locationWFItemMng.getLableItem(xdrItem.stime);
										if (locWFItem != null)
										{
											xdrItem.longitude = locWFItem.longitude;
											xdrItem.latitude = locWFItem.latitude;
											xdrItem.location = ELocationMark.BaiDuSDK.getValue();//
											xdrItem.loctp = ELocationType.Wifi.getName();
											xdrItem.radius = 0;
											xdrItem.latlng_time = xdrItem.stime;
										}
									}
								}
								else if (dataType == XdrLableMapper.DATATYPE_4G_HTTP)
								{
									dataAdapterReader_HTTP.readData(strs);
									if (!xdrItem.FillData_SEQ_HTTP(dataAdapterReader_HTTP))
									{
										continue;
									}

									/*
									 * if (xdrItem.Procedure_Status != 0) { if
									 * (MainModel.GetInstance().getCompile().
									 * Assert(CompileMark.HaErBin)) {
									 * curText.set(ResultHelper.getPutHttpXdr(
									 * xdrItem)); mosMng.write("xdrhttpevent",
									 * NullWritable.get(), curText); } }
									 */

									// LocationItem locItem =
									// locationItemMng.getLableItem(xdrItem.stime);
									// if (xdrItem.longitude == 0 && locItem !=
									// null)
									// {
									// xdrItem.longitude = locItem.longitude;
									// xdrItem.latitude = locItem.latitude;
									// xdrItem.location = locItem.location;
									// xdrItem.loctp = locItem.loctp;
									// xdrItem.radius = locItem.radius;
									// xdrItem.latlng_time = locItem.locTime;
									// xdrItem.wifiName = locItem.wifiName;
									// }

									LocationWFItem locWFItem = locationWFItemMng.getLableItem(xdrItem.stime);
									if (locWFItem != null && xdrItem.longitude <= 0)
									{
										xdrItem.longitude = locWFItem.longitude;
										xdrItem.latitude = locWFItem.latitude;
										xdrItem.location = ELocationMark.BaiDuSDK.getValue();//
										xdrItem.loctp = ELocationType.Wifi.getName();
										xdrItem.radius = 0;
										xdrItem.latlng_time = xdrItem.stime;
									}
								}
							}
							catch (Exception e)
							{
								LOGHelper.GetLogger().writeLog(LogType.error, "xdrItem.FillData error : " + value.toString(), e);
								formatErrCount++;
								continue;
							}

							// 保留原始数据
							xdrItem.valStr = new String(value.toString());

							// 统计算是有经纬度的点个数
							if (xdrItem.Eci > 0 && xdrItem.longitude > 0)
							{
								lteXdrLocCount++;

								MyInt myTemp = cellLocDic.get(xdrItem.Eci);
								if (myTemp == null)
								{
									myTemp = new MyInt(0);
									cellLocDic.put(xdrItem.Eci, myTemp);
								}
								myTemp.data++;
							}

							// dd司机判断
							if (xdrItem.location == 3)
							{
								labelDeal.setDDDriver(true);
							}

							// =============================采样点有效性判断=============================================

							if (xdrItem.IMSI <= 0 || xdrItem.Eci <= 0 || xdrItem.stime <= 0)
							{
								if (xdrItem.longitude > 0)
								{
									curText.set(xdrItem.valStr + "\t" + "数据内容有问题，忽略");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}

								continue;
							}

							LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(xdrItem.Eci);
							int maxRadius = 3000;
							if (lteCellInfo != null)
							{
								if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
								{
									xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
								}
								if (lteCellInfo.radius <= 0)// 没有理想覆盖的数据
								{
									maxRadius = 1000;
								}
								else if (3 * lteCellInfo.radius >= 3000)
								{
									maxRadius = 3000;
								}
								else if (3 * lteCellInfo.radius <= 1000)
								{
									maxRadius = 1000;
								}
								xdrItem.cityID = lteCellInfo.cityid;
							}

							if (xdrItem.cityID < 0)
							{
								if (xdrItem.longitude > 0)
								{
									curText.set(xdrItem.valStr + "\t" + "eci关联配置失败");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}

								eciErrCount++;
								// continue;
							}

							if (xdrItem.dist >= maxRadius && xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "小区距离采样点过远");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
								locErrCount++;
								xdrItem.cleanLoc();
							}

							if (xdrItem.dist < 0 && xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "小区经纬度配置缺失");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
								locErrCount++;
								xdrItem.cleanLoc();
								// continue;
							}

							// 如果是location > 6 属于不可预知的情况,去掉
							if (xdrItem.location > 6)
							{
								if (xdrItem.longitude > 0)
								{
									curText.set(xdrItem.valStr + "\t" + "location不在识别范围内");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}

								continue;
							}

							if (xdrItem.longitude > 0)
							{

								// 如果location 是 0，1 那么将经纬度抹掉
								if (xdrItem.location >= 0 && xdrItem.location <= 1)
								{
									curText.set(xdrItem.valStr + "\t" + "location为01不予分析，忽略");
									if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
										mosMng.write("eventerr", NullWritable.get(), curText);
								}
								else if (xdrItem.location == ELocationMark.BaiDuSDK.getValue())
								{
									if (xdrItem.loctp.length() == 0)
									{
										curText.set(xdrItem.valStr + "\t" + "百度地图的loctp未知");
										if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
											mosMng.write("eventerr", NullWritable.get(), curText);
									}
									else if (xdrItem.loctp.equals(ELocationType.Cell.getName()))
									{
										curText.set(xdrItem.valStr + "\t" + "百度地图的小区定位不可用");
										if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
											mosMng.write("eventerr", NullWritable.get(), curText);
									}
									else if (xdrItem.radius > 100)
									{
										curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
										if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
											mosMng.write("eventerr", NullWritable.get(), curText);
										radiusErrCount++;
									}
								}
								else if (xdrItem.location == ELocationMark.GaoDeSDK.getValue())
								{
									if (!xdrItem.loctp.equals("14") && !xdrItem.loctp.equals("24") && !xdrItem.loctp.equals("5") && !xdrItem.loctp.equals(ELocationType.Wifi.getName())
											&& MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									{
										curText.set(xdrItem.valStr + "\t" + "高德地图的loctp未知");
										mosMng.write("eventerr", NullWritable.get(), curText);
									}
									else if (xdrItem.radius > 100)
									{
										curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
										if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
											mosMng.write("eventerr", NullWritable.get(), curText);
										radiusErrCount++;
									}
								}
								// 腾讯地图loctp需要转换
								else if (xdrItem.location == ELocationMark.TengXunSDK.getValue())
								{
									if (xdrItem.radius > 50)
									{
										// 腾讯定位精度>20的，都没有参考意义
										curText.set(xdrItem.valStr + "\t" + "腾讯地图的定位经度不符合要求，忽略");
										if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
											mosMng.write("eventerr", NullWritable.get(), curText);
									}
								}

							}

							// =============================采样点有效性判断=============================================

							if (curTimeSpan == 0)
							{
								curTimeSpan = xdrItem.stime / TimeSpan * TimeSpan;
							}

							xdrItemList.add(xdrItem);

						}
						else if (dataType == XdrLableMapper.DATATYPE_23G)
						{
							SIGNAL_XDR_23G xdrItem = new SIGNAL_XDR_23G();
							String[] datas = strs.split("\t", -1);
							LocInfoItem locInfoItem = new LocInfoItem();
							try
							{
								if (!locInfoItem.FillData(datas, 0))
								{
									continue;
								}

								// 保留原始数据
								xdrItem.valStr = new String(value.toString());

								xdrItem.Session_ID = locInfoItem.session_id;
								xdrItem.Online_ID = locInfoItem.online_id;
								xdrItem.eventType = locInfoItem.event_type;
								xdrItem.imsi = locInfoItem.imsi;

								GisPos gisPos = GisLocater.bd09_To_Gps84(locInfoItem.longitude, locInfoItem.latitude);

								if (gisPos.getWgLon() > 180 || gisPos.getWgLon() < 0)
								{
									xdrItem.longitude = 0;
								}
								if (gisPos.getWgLat() > 90 || gisPos.getWgLat() < 0)
								{
									xdrItem.latitude = 0;
								}

								xdrItem.longitude = (int) (gisPos.getWgLon() * 10000000D);
								xdrItem.latitude = (int) (gisPos.getWgLat() * 10000000D);

								xdrItem.location = locInfoItem.location;
								xdrItem.dist = locInfoItem.dist;
								xdrItem.radius = locInfoItem.radius;
								xdrItem.loctp = locInfoItem.loctp;
								// xdrItem.indoor = locationItem.indoor;
								xdrItem.nettype = locInfoItem.networktype;
								String[] tmval = locInfoItem.lac_ci.split("_");
								xdrItem.lac = Integer.parseInt(tmval[0]);
								xdrItem.ci = Integer.parseInt(tmval[1]);
								xdrItem.uenettype = locInfoItem.uenettype;

								Date d_beginTime = format.parse(locInfoItem.timeStr);
								xdrItem.stime = (int) (d_beginTime.getTime() / 1000L);

								xdrItem.lockNetMark = locInfoItem.lockNetMark;
							}
							catch (Exception e)
							{
								LOGHelper.GetLogger().writeLog(LogType.error, "locationItem.FillData error : " + value.toString(), e);
								formatErrCount++;
								continue;
							}

							// dd司机判断
							if (xdrItem.location == 3)
							{
								labelDeal.setDDDriver(true);
								// LOGHelper.GetLogger().writeLog(LogType.info,
								// "find dd driver user : " + value.toString());
							}

							// =============================采样点有效性判断=============================================
							if (xdrItem.imsi <= 0 || xdrItem.lac <= 0 || xdrItem.ci <= 0 || xdrItem.stime <= 0)
							{
								LOGHelper.GetLogger().writeLog(LogType.warn, "23G data not valid : " + value);
								continue;
							}

							int maxRadius = 3000;
							if (xdrItem.nettype == 1)// gsm
							{
								GsmCellInfo cellInfo = CellConfig.GetInstance().getGsmCell(xdrItem.lac, xdrItem.ci);
								if (cellInfo != null)
								{
									if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && cellInfo.ilongitude > 0 && cellInfo.ilatitude > 0)
									{
										xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude, cellInfo.ilongitude, cellInfo.ilatitude);
									}
									if (cellInfo.radius <= 0)// 没有理想覆盖的数据
									{
										maxRadius = 1000;
									}
									else if (3 * cellInfo.radius >= 3000)
									{
										maxRadius = 3000;
									}
									else if (3 * cellInfo.radius <= 1000)
									{
										maxRadius = 1000;
									}

									xdrItem.cityID = cellInfo.cityid;
								}
								else
								{
									LOGHelper.GetLogger().writeLog(LogType.warn, "23G location gsm cell not found : " + xdrItem.lac + " " + xdrItem.ci);
								}
							}
							else if (xdrItem.nettype == 2)// tdscdma
							{
								TdCellInfo cellInfo = CellConfig.GetInstance().getTdCell(xdrItem.lac, xdrItem.ci);
								if (cellInfo != null)
								{
									if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && cellInfo.ilongitude > 0 && cellInfo.ilatitude > 0)
									{
										xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude, cellInfo.ilongitude, cellInfo.ilatitude);
									}

									if (cellInfo.radius <= 0)// 没有理想覆盖的数据
									{
										maxRadius = 1000;
									}
									else if (3 * cellInfo.radius >= 3000)
									{
										maxRadius = 3000;
									}
									else if (3 * cellInfo.radius <= 1000)
									{
										maxRadius = 1000;
									}

									xdrItem.cityID = cellInfo.cityid;
								}
								else
								{
									LOGHelper.GetLogger().writeLog(LogType.warn, "23G location tdscdma cell not found : " + xdrItem.lac + " " + xdrItem.ci);
								}
							}
							else
							{
								curText.set(xdrItem.valStr + "\t" + "网络类型未知：" + xdrItem.nettype);
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr23g", NullWritable.get(), curText);
							}

							if (xdrItem.cityID <= 0)
							{
								curText.set(xdrItem.valStr + "\t" + "lac ci 找不到地市id：" + xdrItem.lac + "_" + xdrItem.ci);
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr23g", NullWritable.get(), curText);
								eciErrCount_23g++;
								continue;
							}

							if (xdrItem.dist >= maxRadius && xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "小区距离采样点过远：" + xdrItem.lac + "_" + xdrItem.ci + " 距离: " + xdrItem.dist);
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr23g", NullWritable.get(), curText);
								locErrCount_23g++;
								continue;
							}

							if (xdrItem.longitude > 0 && xdrItem.radius > 100)
							{
								curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr23g", NullWritable.get(), curText);
								radiusErrCount_23g++;
							}

							// 如果是location > 6 属于不可预知的情况,去掉
							if (xdrItem.location > 6)
							{
								curText.set(xdrItem.valStr + "\t" + "location不在识别范围内：" + xdrItem.location);
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr23g", NullWritable.get(), curText);

								continue;
							}

							// =============================采样点有效性判断=============================================

							if (curTimeSpan == 0)
							{
								curTimeSpan = xdrItem.stime / TimeSpan * TimeSpan;
							}

							xdrItemList.add(xdrItem);
						}
					}

					// 基于gps信息自增相关的xdr
					List<LocationItem> locItemList = locationItemMng.getLocSpan(curTimeSpan);
					if (locItemList != null)
					{
						for (LocationItem locItem : locItemList)// 把location中的位置信息保存
						{
							SIGNAL_XDR_4G xdrItem = new SIGNAL_XDR_4G();
							xdrItem.IMSI = locItem.imsi;
							xdrItem.stime = locItem.locTime;
							xdrItem.Eci = locItem.eci;
							xdrItem.Event_Type = 100;

							xdrItem.longitude = locItem.longitude;
							xdrItem.latitude = locItem.latitude;
							xdrItem.location = locItem.location;
							xdrItem.loctp = locItem.loctp;
							xdrItem.radius = locItem.radius;
							xdrItem.latlng_time = locItem.locTime;
							xdrItem.wifiName = locItem.wifiName;

							xdrItemList.add(xdrItem);
						}
					}
					labelDeal.deal(xdrItemList);
					outDealingData(xdrItemList);
					xdrItemList.clear();
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "xdrlableFileReduce  error ", e);
			}
		}

		/**
		 * 半小时粒度进行经纬度回填
		 * 
		 * @param xdrItemList
		 *            用户全天的xdr
		 * @throws InterruptedException
		 * @throws IOException
		 */
		public void dealEstiLoc(List<SIGNAL_LOC> xdrItemList)
		{
			int timeSpan = 1800;// 30min
			List<SIGNAL_LOC> xdrItemList_30Min = new ArrayList<SIGNAL_LOC>();
			boolean ifUpLoc = false;// 是否上报过位置
			int stime = 0;

			// 用户全天的数据按时间排序
			Collections.sort(xdrItemList);
			for (SIGNAL_LOC tempXdr : xdrItemList)
			{
				if (tempXdr.stime / timeSpan * timeSpan == stime)
				{
					if (tempXdr.longitude > 0 && ifUpLoc == false)
					{
						ifUpLoc = true;// 上报过位置
						xdrItemList_30Min.clear();// 清空
					}
					else if (ifUpLoc == false)
					{
						xdrItemList_30Min.add(tempXdr);
					}
				}
				else
				{
					if (locationImsiCellLocMng.getImsiCellLocMap().size() > 0 && xdrItemList_30Min.size() > 0)
					{
						labelDeal.estiLabelDeal(xdrItemList_30Min, locationImsiCellLocMng);
						xdrItemList_30Min.clear();
					}
					stime = tempXdr.stime / timeSpan * timeSpan;
					if (tempXdr.longitude > 0)
					{
						ifUpLoc = true;
					}
					else
					{
						ifUpLoc = false;
						xdrItemList_30Min.add(tempXdr);
					}
				}
			}
		}

		// 吐出用户过程数据，为了防止内存过多
		private StringBuilder tmSb = new StringBuilder();

		private void outDealingData(List<SIGNAL_LOC> xdrList)
		{
			userXdrList.addAll(xdrList);

		}

		public void dealUserData(List<SIGNAL_LOC> xdrItemList)
		{
			if (xdrItemList.size() == 0)
			{
				return;
			}

			labelDeal.finalDeal();
			// 用户常驻小区经纬度回填
			dealEstiLoc(xdrItemList);

			TestTypeDeal testTypeDeal = new TestTypeDeal(curImsi, labelDeal.IsDDDriver(), labelDeal.getUserHomeCellMap());

			testTypeDeal.deal(xdrItemList);

			dealSample(xdrItemList);
		}

		// 将会吐出用户最后所有数据
		private void outUserData()
		{
			dealUserData(userXdrList);

			// 生成用户事件块索引表
			// for (HourDataDeal gridTimeDeal :
			// gridDeal.getHourDataDealMap().values())
			// {
			// for (ImsiBlockKey imsiBlockKey :
			// gridTimeDeal.getImsiBlockEventMap().keySet())
			// {
			// try
			// {
			// curText.set(ResultHelper.getPutImsiEventIndex(imsiBlockKey.getImsi(),gridTimeDeal.getTimeSpan(),
			// imsiBlockKey.getBlock()));
			// mosMng.write("imsieventindex", NullWritable.get(), curText);
			// }
			// catch (Exception e)
			// {
			// // TODO: handle exception
			// }
			// }
			// gridTimeDeal.getImsiBlockEventMap().clear();
			// }

			// dt
			// 生成用户事件块索引表
			// for (HourDataDeal gridTimeDeal :
			// gridDeal_DT.getHourDataDealMap().values())
			// {
			// for (ImsiBlockKey imsiBlockKey :
			// gridTimeDeal.getImsiBlockEventMap().keySet())
			// {
			// try
			// {
			// curText.set(ResultHelper.getPutImsiEventIndex(imsiBlockKey.getImsi(),gridTimeDeal.getTimeSpan(),
			// imsiBlockKey.getBlock()));
			// mosMng.write("eventindexdt", NullWritable.get(), curText);
			// }
			// catch (Exception e)
			// {
			// // TODO: handle exception
			// }
			// }
			// gridTimeDeal.getImsiBlockEventMap().clear();
			// }

			// cqt
			// 生成用户事件块索引表
			// for (HourDataDeal gridTimeDeal :
			// gridDeal_CQT.getHourDataDealMap().values())
			// {
			// for (ImsiBlockKey imsiBlockKey :
			// gridTimeDeal.getImsiBlockEventMap().keySet())
			// {
			// try
			// {
			// curText.set(ResultHelper.getPutImsiEventIndex(imsiBlockKey.getImsi(),gridTimeDeal.getTimeSpan(),
			// imsiBlockKey.getBlock()));
			// mosMng.write("eventindexcqt", NullWritable.get(), curText);
			// }
			// catch (Exception e)
			// {
			// // TODO: handle exception
			// }
			// }
			// gridTimeDeal.getImsiBlockEventMap().clear();
			// }

			statDeal.outDealingData();
			statDeal_DT.outDealingData();
			statDeal_CQT.outDealingData();

			// 如果用户数据大于10000个，就吐出去先
			if (userInfoMng.getUserInfoMap().size() > 10000)
			{
				userInfoMng.finalStat();

				// 用户统计信息输出
				for (UserInfo userInfo : userInfoMng.getUserInfoMap().values())
				{
					try
					{
						curText.set(ResultHelper.getPutUerInfo(userInfo));
						mosMng.write("userinfo", NullWritable.get(), curText);
					}
					catch (Exception e)
					{
						// TODO: handle exception
					}
				}

				// 用户行动信息输出
				for (UserActStat userActStat : userInfoMng.getUserActStatMap().values())
				{
					try
					{
						StringBuffer sb = new StringBuffer();
						String TabMark = "\t";
						for (UserActTime userActTime : userActStat.userActTimeMap.values())
						{
							for (UserAct userAct : userActTime.userActMap.values())
							{
								sb.delete(0, sb.length());

								sb.append(0);
								sb.append(TabMark); // cityid
								sb.append(userActStat.imsi);
								sb.append(TabMark);
								sb.append(userActStat.msisdn);
								sb.append(TabMark);
								sb.append(userActTime.stime);
								sb.append(TabMark);
								sb.append(userActTime.etime);
								sb.append(TabMark);
								sb.append(userAct.eci);
								sb.append(TabMark);
								sb.append(userAct.sn);
								sb.append(TabMark);
								sb.append(userActTime.cellcount);
								sb.append(TabMark);
								sb.append(userActTime.xdrcountTotal);
								sb.append(TabMark);
								sb.append(userActTime.duration);
								sb.append(TabMark);
								sb.append(userAct.xdrcount);
								sb.append(TabMark);
								sb.append(userAct.cellduration);
								sb.append(TabMark);
								sb.append(doubleFormat.format(userAct.durationrate));

								curText.set(sb.toString());
								mosMng.write("useract", NullWritable.get(), curText);
							}
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "user action error", e);
					}
				}

				userInfoMng = new UserInfoMng();
			}

		}

		// 将吐出程序结束前滞留的数据
		private void outAllData()
		{
			outUserData();

			statDeal.outAllData();
			statDeal_DT.outAllData();
			statDeal_CQT.outAllData();

			// 用户统计信息输出
			userInfoMng.finalStat();

			for (UserInfo userInfo : userInfoMng.getUserInfoMap().values())
			{
				try
				{
					curText.set(ResultHelper.getPutUerInfo(userInfo));
					mosMng.write("userinfo", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
				}
			}

			for (UserActStat userActStat : userInfoMng.getUserActStatMap().values())
			{
				try
				{
					StringBuffer sb = new StringBuffer();
					String TabMark = "\t";
					for (UserActTime userActTime : userActStat.userActTimeMap.values())
					{
						for (UserAct userAct : userActTime.userActMap.values())
						{
							sb.delete(0, sb.length());

							sb.append(0);
							sb.append(TabMark); // cityid
							sb.append(userActStat.imsi);
							sb.append(TabMark);
							sb.append(userActStat.msisdn);
							sb.append(TabMark);
							sb.append(userActTime.stime);
							sb.append(TabMark);
							sb.append(userActTime.etime);
							sb.append(TabMark);
							sb.append(userAct.eci);
							sb.append(TabMark);
							sb.append(userAct.sn);
							sb.append(TabMark);
							sb.append(userActTime.cellcount);
							sb.append(TabMark);
							sb.append(userActTime.xdrcountTotal);
							sb.append(TabMark);
							sb.append(userActTime.duration);
							sb.append(TabMark);
							sb.append(userAct.xdrcount);
							sb.append(TabMark);
							sb.append(userAct.cellduration);
							sb.append(TabMark);
							sb.append(doubleFormat.format(userAct.durationrate));

							curText.set(sb.toString());
							mosMng.write("useract", NullWritable.get(), curText);
						}
					}
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "user action error", e);
				}
			}

		}

		public void dealCPEData(List<SIGNAL_LOC> xdrItemList)
		{
			if (xdrItemList.size() == 0)
			{
				return;
			}

			DT_Sample_4G sample = new DT_Sample_4G();

			for (SIGNAL_LOC locItem : xdrItemList)
			{
				labelDeal.formatData(locItem);

				if (locItem.longitude > 0)
				{
					locItem.testType = StaticConfig.TestType_CPE;

					locItem.longitudeGL = locItem.longitude;
					locItem.latitudeGL = locItem.latitude;
					locItem.testTypeGL = locItem.testType;

					locItem.locationGL = locItem.location;
					locItem.distGL = locItem.dist;
					locItem.radiusGL = locItem.radius;
					locItem.loctpGL = locItem.loctp;
					locItem.indoorGL = 0;
					locItem.lableGL = locItem.mt_label;
					locItem.loctimeGL = locItem.stime;

					sample.Clear();
					statXdr_4G(sample, (SIGNAL_XDR_4G) locItem);
				}
				userInfoMng.stat(sample);
			}
		}

		/**
		 * 用户小区切换列表
		 * 
		 * @param xdrList
		 *            用户全天xdr数据
		 * @return
		 */
		public ArrayList<cellSwitInfo> getCellSwitList(List<SIGNAL_LOC> xdrList)
		{
			ArrayList<cellSwitInfo> cellSwitList = new ArrayList<cellSwitInfo>();// 这个用户的小区切换列表
			cellSwitInfo cellInfo = null;
			cellSwitInfo tempCellInfo = new cellSwitInfo();
			for (SIGNAL_LOC data : xdrList)
			{
				SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) data;
				if (xdrItem.stime <= 0)
				{
					continue;
				}
				cellInfo = new cellSwitInfo(xdrItem.IMSI, xdrItem.Eci, xdrItem.stime, xdrItem.stime, xdrItem.MSISDN);
				if (cellSwitList.size() > 0)
				{
					tempCellInfo = cellSwitList.get(cellSwitList.size() - 1);// list中最后一个cellInfo;
				}
				if (cellInfo.eci == tempCellInfo.eci)
				{
					tempCellInfo.etime = cellInfo.stime;
					if (xdrItem.longitude > 0)
					{
						tempCellInfo.longtitude += xdrItem.longitude;
						tempCellInfo.latitude += xdrItem.latitude;
						tempCellInfo.num++;
					}
				}
				else
				{
					cellSwitList.add(cellInfo);
					if (xdrItem.longitude > 0)
					{
						cellInfo.longtitude += xdrItem.longitude;
						cellInfo.latitude += xdrItem.latitude;
						cellInfo.num++;
					}
				}
			}

			return cellSwitList;
		}

		/**
		 * 用户在小区每个小时停留时长
		 * 
		 * @param cellSwitList
		 *            小区切换列表
		 */
		public void userCellTimePerHour(ArrayList<cellSwitInfo> cellSwitList)
		{
			// 小时小区时间统计
			HashMap<CellTimeKey, LocationImsiCellTime> usrcellHourMap = new HashMap<CellTimeKey, LocationImsiCellTime>();
			LocationImsiCellTime temp = null;
			for (cellSwitInfo cellInfo : cellSwitList)
			{
				long sdayhour = Util.RoundTimeForHour(cellInfo.stime);
				long edayhour = Util.RoundTimeForHour(cellInfo.etime);
				CellTimeKey key = null;
				long durTimes = 0;
				if (sdayhour == edayhour)// 没有小时跨度
				{
					key = new CellTimeKey(cellInfo.eci, sdayhour);
					durTimes = cellInfo.etime - cellInfo.stime;// 停留时间
					temp = usrcellHourMap.get(key);
					if (temp == null)
					{
						temp = new LocationImsiCellTime(cellInfo.imsi, cellInfo.eci, sdayhour, durTimes, cellInfo.msisdn);
						usrcellHourMap.put(key, temp);
					}
					else
					{
						temp.times += durTimes;
					}
					if (cellInfo.longtitude > 0)
					{
						temp.longtitude += cellInfo.longtitude;
						temp.latitude += cellInfo.latitude;
						temp.num += cellInfo.num;
					}
				}
				else// 有小时跨度
				{
					int durHour = (int) (edayhour - sdayhour) / 3600;// 间隔n个小时
					for (int i = 0; i <= durHour; i++)
					{
						key = new CellTimeKey(cellInfo.eci, sdayhour + i * 3600);
						temp = usrcellHourMap.get(key);
						if (i == 0)
						{
							durTimes = 3600 - cellInfo.stime % (24 * 3600) % 3600;
						}
						else if (i == durHour)
						{
							durTimes = cellInfo.etime % (24 * 3600) % 3600;
						}
						else
						{
							durTimes = 3600;
						}
						if (temp == null)
						{
							temp = new LocationImsiCellTime(cellInfo.imsi, cellInfo.eci, sdayhour + i * 3600, durTimes, cellInfo.msisdn);
							usrcellHourMap.put(key, temp);
						}
						else
						{
							temp.times += durTimes;
						}
						if (cellInfo.longtitude > 0)
						{
							temp.longtitude += cellInfo.longtitude;
							temp.latitude += cellInfo.latitude;
							temp.num += cellInfo.num;
						}
					}
				}
			}
			// 写数据
			for (LocationImsiCellTime cellTime : usrcellHourMap.values())
			{
				curText.set(cellTime.toString());
				try
				{
					mosMng.write("cellhourTime", NullWritable.get(), curText);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "imsiCellTimeWrite Err", e);
				}
			}
		}

		/**
		 * 确定同一个用户前后5分钟的小区切换
		 * 
		 * @param xdrList
		 *            该用户全天的xdr数据
		 * @param cellSwitList
		 *            该用户小区切换列表
		 * 
		 */
		public void get5MinEciList(ArrayList<cellSwitInfo> cellSwitList, List<SIGNAL_LOC> xdrList)
		{
			// 确定 xdr前后五分钟的小区切换信息
			int xdrpos = 0;
			for (cellSwitInfo tempSwitCell : cellSwitList)// eciSwitch
			{
				for (int i = xdrpos; i < xdrList.size(); i++)
				{
					SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) xdrList.get(i);
					int xdrStime = xdrItem.stime - 300;
					int xdrEtime = xdrItem.stime + 300;
					if (tempSwitCell.etime < xdrStime)
					{
						break;
					}
					if (tempSwitCell.stime > xdrEtime)
					{
						xdrpos++;
					}
					if (xdrStime <= tempSwitCell.etime && xdrEtime >= tempSwitCell.stime)// 时间有交集
					{
						if (xdrItem.eciSwitchList.length() < 1)
						{
							xdrItem.eciSwitchList.append(tempSwitCell.toString());
						}
						else
						{
							xdrItem.eciSwitchList.append(";" + tempSwitCell.toString());
						}
					}
				}
			}
		}

		/**
		 * 高铁定位
		 * 
		 * @param xdrList
		 *            用户全天xdr 包括mme和http
		 */
		public void doHiRailFix(List<SIGNAL_LOC> xdrList)
		{
			ArrayList<SIGNAL_XDR_4G> allList = new ArrayList<SIGNAL_XDR_4G>();
			for (SIGNAL_LOC temp : xdrList)
			{
				SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) temp;
				allList.add(xdrItem);
			}
			RailFillFunc railFillFunc = new RailFillFunc();
			railFillFunc.deal(allList);
		}

		private void dealSample(List<SIGNAL_LOC> xdrList)
		{
			DT_Sample_4G sample = new DT_Sample_4G();
			DT_Sample_23G sample_23g = new DT_Sample_23G();

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.EstiLoc) || MainModel.GetInstance().getCompile().Assert(CompileMark.BA5MinCell))
			{
				ArrayList<cellSwitInfo> cellSwitList = getCellSwitList(xdrList);// 得到小区切换列表
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.EstiLoc))// 得到用户每个小时在小区停留时间
				{
					userCellTimePerHour(cellSwitList);
				}
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BA5MinCell))
				{
					get5MinEciList(cellSwitList, xdrList);
				}
			}
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail))// 识别高铁用户
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "begin fix  HiRail ... ...");
				doHiRailFix(xdrList);
			}
			for (SIGNAL_LOC data : xdrList)
			{
				if (data.testType == StaticConfig.TestType_ERROR)
				{
					continue;
				}

				// 如果是4g数据，就按照4g的数据处理
				if (data instanceof SIGNAL_XDR_4G)
				{
					sample.Clear();

					statXdr_4G(sample, (SIGNAL_XDR_4G) data);
					statKpi_4G(sample);
					userInfoMng.stat(sample);
				}
				// 如果是23g数据，就按照23g的数据处理
				else if (data instanceof SIGNAL_XDR_23G)
				{
					sample_23g.Clear();

					statXdr_23G(sample_23g, (SIGNAL_XDR_23G) data);
					statKpi_23G(sample_23g);
				}
			}
		}

		private void statKpi_4G(DT_Sample_4G sample)
		{
			statDeal.dealSample(sample);
			statDeal_DT.dealSample(sample);
			statDeal_CQT.dealSample(sample);
		}

		private void statKpi_23G(DT_Sample_23G sample)
		{
			statDeal.dealSample(sample);
			statDeal_DT.dealSample(sample);
			statDeal_CQT.dealSample(sample);
		}

		private void statXdr_4G(DT_Sample_4G tsam, SIGNAL_XDR_4G tTemp)
		{
			// sample
			tsam.ilongitude = tTemp.longitude;
			tsam.ilatitude = tTemp.latitude;

			tsam.ispeed = (tTemp.stime_ms >> 16); // 标识经纬度来源
			tsam.cityID = tTemp.cityID;
			tsam.itime = tTemp.stime;
			tsam.wtimems = (short) (tTemp.stime_ms);
			tsam.iLAC = (int) getValidData(tsam.iLAC, tTemp.TAC);
			tsam.iCI = (long) getValidData(tsam.iCI, tTemp.CI);
			tsam.Eci = (long) getValidData(tsam.Eci, tTemp.Eci);

			if (tTemp.MSISDN.length() != 0)
			{
				tsam.MSISDN = tTemp.MSISDN;
			}
			if (tTemp.IMEI.length() > 10)
			{
				tsam.UETac = tTemp.IMEI.substring(0, 7) + "0";
			}
			if (tTemp.Brand.length() != 0)
			{
				tsam.UEBrand = tTemp.Brand;
			}
			if (tTemp.Type.length() != 0)
			{
				tsam.UEType = tTemp.Type;
			}

			tsam.serviceType = tTemp.Service_Type;
			tsam.serviceSubType = tTemp.Sub_Service_Type;
			tsam.urlDomain = tTemp.Referer;
			tsam.IPDataUL = tTemp.IP_Data_Len_UL;
			tsam.IPDataDL = tTemp.IP_Data_Len_DL;
			tsam.IPPacketUL = tTemp.Count_Packet_UL;
			tsam.IPPacketDL = tTemp.Count_Packet_DL;
			// tsam.duration = tTemp.Duration;

			// result == 1 才是正常合成的事件
			tsam.IPThroughputUL = 0;
			tsam.IPThroughputDL = 0;
			tsam.duration = 0;
			if (tTemp.Result == 1)
			{
				tsam.duration = tTemp.Result_DelayFirst;
				if (tTemp.Result_DelayFirst > 0)
				{
					tsam.IPThroughputUL = (double) (tTemp.IP_Data_Len_UL * 8.0 / (tTemp.Result_DelayFirst / 1000.0)) / 1024;
					tsam.IPThroughputDL = (double) (tTemp.IP_Data_Len_DL * 8.0 / (tTemp.Result_DelayFirst / 1000.0)) / 1024;
				}
			}

			tsam.TCPReTranPacketUL = tTemp.Retran_Packet_UL;
			tsam.TCPReTranPacketDL = tTemp.Retran_Packet_DL;
			tsam.sessionRequest = 1;
			tsam.sessionResult = tTemp.Result;
			tsam.eventType = tTemp.Event_Type;

			tsam.eNBName = tTemp.ENB;
			// tsam.eNBLongitude = tTemp.longitude;
			// tsam.eNBLatitude = tTemp.latitude;
			tsam.flag = "EVT";
			if (tTemp.CI != 0 && tTemp.CI != -1000000)
			{
				tsam.ENBId = (int) (tTemp.CI / 256);
			}
			tsam.CellId = (long) getValidData(tsam.CellId, tTemp.CI);
			tsam.IMSI = (long) getValidData(tsam.IMSI, tTemp.IMSI);
			tsam.MmeCode = (int) getValidData(tsam.MmeCode, tTemp.MME_CODE);
			tsam.MmeGroupId = (int) getValidData(tsam.MmeGroupId, tTemp.MME_GROUP_ID);
			tsam.MmeUeS1apId = (Long) getValidData(tsam.MmeUeS1apId, tTemp.MME_UE_S1AP_ID);
			tsam.LocFillType = tTemp.LocFillType;

			tsam.location = tTemp.location;
			tsam.dist = tTemp.dist;
			tsam.radius = tTemp.radius;
			tsam.loctp = tTemp.loctp;
			// tsam.indoor = tTemp.indoor;
			tsam.testType = tTemp.testType;

			tsam.networktype = tTemp.networktype;
			// tsam.lable = tTemp.lable;
			tsam.indoor = (int) tTemp.mt_speed;
			tsam.lable = tTemp.mt_label;
			// 吐出xdr location
			try
			{
				XdrLable xdrlocation = new XdrLable();
				xdrlocation.cityID = tsam.cityID;
				xdrlocation.eci = tsam.Eci;
				xdrlocation.s1apid = tsam.MmeUeS1apId;
				xdrlocation.itime = tsam.itime;
				xdrlocation.imsi = tsam.IMSI;
				xdrlocation.ilongtude = tsam.ilongitude;
				xdrlocation.ilatitude = tsam.ilatitude;
				xdrlocation.testType = tsam.testType;
				xdrlocation.location = tsam.location;
				xdrlocation.dist = tsam.dist;
				xdrlocation.radius = tsam.radius;
				xdrlocation.loctp = tsam.loctp;
				xdrlocation.indoor = tsam.indoor;
				xdrlocation.networktype = tsam.networktype;
				xdrlocation.lable = tsam.lable;
				xdrlocation.longitudeGL = tTemp.longitudeGL;
				xdrlocation.latitudeGL = tTemp.latitudeGL;
				xdrlocation.testTypeGL = tTemp.testTypeGL;
				xdrlocation.locationGL = tTemp.locationGL;
				xdrlocation.distGL = tTemp.distGL;
				xdrlocation.radiusGL = tTemp.radiusGL;
				xdrlocation.loctpGL = tTemp.loctpGL;
				xdrlocation.indoorGL = tTemp.indoorGL;
				xdrlocation.lableGL = tTemp.lableGL;
				xdrlocation.serviceType = tTemp.Service_Type;
				xdrlocation.subServiceType = tTemp.Sub_Service_Type;
				xdrlocation.moveDirect = tTemp.moveDirect;
				xdrlocation.loctimeGL = tTemp.loctimeGL;
				xdrlocation.host = tTemp.host;
				xdrlocation.wifiName = tTemp.wifiName;
				try
				{
					xdrlocation.imeiTac = Integer.parseInt(tTemp.IMEI.substring(0, 8));
				}
				catch (Exception e)
				{
					xdrlocation.imeiTac = 0;
				}
				xdrlocation.eciSwitchList = tTemp.eciSwitchList.toString();
				xdrlocation.areaType = tTemp.areaType;
				xdrlocation.areaId = tTemp.areaId;
				xdrlocation.msisdn = tTemp.MSISDN;

				if (tTemp.testType == StaticConfig.TestType_HiRail)
				{
					curText.set(xdrlocation.toString());
					mosMng.write("hirailxdr", NullWritable.get(), curText);
				}
				curText.set(xdrlocation.toString());
				mosMng.write("xdrLocation", NullWritable.get(), curText);
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}

			// Event Output
			DT_Event tEvt = new DT_Event();
			tEvt.Clear();
			tEvt.Procedure_Status = tTemp.Procedure_Status;
			tEvt.imsi = tsam.IMSI;
			tEvt.SampleID = tsam.imeiTac + 1;
			tEvt.ilongitude = tsam.ilongitude;
			tEvt.ilatitude = tsam.ilatitude;

			tEvt.cityID = tTemp.cityID;
			tEvt.projectID = 99999999;
			tEvt.itime = tTemp.stime;
			tEvt.wtimems = (short) (tTemp.stime_ms);
			tEvt.eventID = tTemp.Event_Type < 100 ? 29000 + tTemp.Event_Type : tTemp.Event_Type + 20000;
			tEvt.iLAC = tTemp.TAC;
			// tEvt.iCI = tTemp.CI;
			// 存放eci，原始ici不是标准格式
			tEvt.iCI = (long) tTemp.Eci;
			tEvt.ivalue1 = tTemp.Service_Type;
			tEvt.ivalue2 = tTemp.Sub_Service_Type;
			// tEvt.ivalue3 = tTemp.IMSI;

			tEvt.ivalue4 = 0;
			tEvt.ivalue5 = 0;
			tEvt.ivalue6 = 0;
			if (tTemp.Result == 1)
			{
				if (tTemp.Result_DelayFirst > 0)
				{
					tEvt.ivalue4 = Math.round(tTemp.IP_Data_Len_UL * 8.0 / (tTemp.Result_DelayFirst / 1000.0));
					tEvt.ivalue5 = Math.round(tTemp.IP_Data_Len_DL * 8.0 / (tTemp.Result_DelayFirst / 1000.0));
				}
				tEvt.ivalue6 = tTemp.Result_DelayFirst;
			}

			tEvt.ivalue7 = tTemp.MME_UE_S1AP_ID;
			tEvt.cqtposid = (tTemp.stime_ms >> 16); // 标识经纬度来源
			tEvt.LocFillType = tTemp.LocFillType;

			// 用带采样点的赋值
			tEvt.testType = tTemp.testType;
			tEvt.location = tTemp.location;
			tEvt.dist = tTemp.dist;
			tEvt.radius = tTemp.radius;
			tEvt.loctp = tTemp.loctp;

			// tEvt.indoor = tTemp.indoor;
			tEvt.networktype = tTemp.networktype;
			// tEvt.lable = tTemp.lable;
			tEvt.indoor = (int) tTemp.mt_speed;
			tEvt.lable = tTemp.mt_label;

			tEvt.moveDirect = tTemp.moveDirect;

			// 低速率事件
			if ((tEvt.ivalue5 < 1024 * 1024) && ((tEvt.ivalue5 / 8.0 * (tEvt.ivalue6 / 1000.0)) > 1 * 1024 * 1024))
			{
				tEvt.SampleID += 1;
				tEvt.eventID = 28001;

				outEvent(tEvt);
			}
			else
			{
				outEvent(tEvt);
			}

		}

		private void outEvent(DT_Event tEvt)
		{
			try
			{
				// 特殊用户吐出全量的event
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) && SpecialUserConfig.GetInstance().ifSpeciUser(tEvt.imsi, false))
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("evtVap", NullWritable.get(), curText);
				}

				if (tEvt.testType == StaticConfig.TestType_DT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdt", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_DT_EX || tEvt.testType == StaticConfig.TestType_CPE)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdtex", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_CQT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventcqt", NullWritable.get(), curText);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "outEvent error ", e);
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

		private void statXdr_23G(DT_Sample_23G tsam, SIGNAL_XDR_23G tTemp)
		{
			// sample
			tsam.ilongitude = tTemp.longitude;
			tsam.ilatitude = tTemp.latitude;

			tsam.ispeed = (tTemp.stime_ms >> 16); // 标识经纬度来源
			tsam.cityID = tTemp.cityID;
			tsam.itime = tTemp.stime;
			tsam.wtimems = (short) (tTemp.stime_ms);
			tsam.iLAC = (int) getValidData(tsam.iLAC, tTemp.lac);
			tsam.iCI = (int) getValidData(tsam.iCI, tTemp.ci);

			tsam.eventType = tTemp.eventType;

			tsam.flag = "EVT";

			tsam.CellId = (int) getValidData(tsam.CellId, tTemp.ci);
			tsam.IMSI = (Long) getValidData(tsam.IMSI, tTemp.imsi);

			tsam.location = tTemp.location;
			tsam.dist = tTemp.dist;
			tsam.radius = tTemp.radius;
			tsam.loctp = tTemp.loctp;
			// tsam.indoor = tTemp.indoor;
			tsam.testType = tTemp.testType;

			tsam.networktype = tTemp.networktype;
			// tsam.lable = tTemp.lable;
			tsam.indoor = (int) tTemp.mt_speed;
			tsam.lable = tTemp.mt_label;

			tsam.nettypeFB = tTemp.nettype;

			// Event Output
			DT_Event tEvt = new DT_Event();
			tEvt.Clear();
			tEvt.Procedure_Status = 0;
			tEvt.imsi = tsam.IMSI;
			tEvt.SampleID = tsam.SampleID + 1;
			tEvt.ilongitude = tsam.ilongitude;
			tEvt.ilatitude = tsam.ilatitude;

			tEvt.cityID = tTemp.cityID;
			tEvt.projectID = 99999999;
			tEvt.itime = tTemp.stime;
			tEvt.wtimems = (short) (tTemp.stime_ms);
			tEvt.eventID = tTemp.eventType < 100 ? 29000 + tTemp.eventType : tTemp.eventType + 20000;
			tEvt.iLAC = tTemp.lac;
			tEvt.iCI = tTemp.ci;

			tEvt.ivalue1 = tTemp.nettype;
			tEvt.ivalue2 = tTemp.lockNetMark;

			// 用带采样点的赋值
			tEvt.testType = tTemp.testType;
			tEvt.location = tTemp.location;
			tEvt.dist = tTemp.dist;
			tEvt.radius = tTemp.radius;
			tEvt.loctp = tTemp.loctp;

			// tEvt.indoor = tTemp.indoor;
			tEvt.networktype = tTemp.uenettype;
			tEvt.indoor = (int) tTemp.mt_speed;
			tEvt.lable = tTemp.mt_label;

			tEvt.moveDirect = tTemp.moveDirect;

			outEvent_23G(tEvt);
		}

		private void outEvent_23G(DT_Event tEvt)
		{
			try
			{
				// curText.set(ResultHelper.getPutLteEvent(tEvt));
				// mosMng.write("xdrevent", NullWritable.get(), curText);

				if (tEvt.testType == StaticConfig.TestType_DT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdt23g", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_DT_EX)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdtex23g", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_CQT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventcqt23g", NullWritable.get(), curText);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "outEvent error ", e);
			}

		}

	}

	public static class cellSwitInfo
	{
		public long imsi;
		public long eci;
		public int stime;
		public int etime;
		public long longtitude;
		public long latitude;
		public int num;// 在这个小区上报了几个位置
		public String msisdn;// 用户号码

		public cellSwitInfo()
		{
		}

		public cellSwitInfo(long imsi, long eci, int stime, int etime, String msisdn)
		{
			this.imsi = imsi;
			this.eci = eci;
			this.stime = stime;
			this.etime = etime;
			this.msisdn = msisdn;
		}

		public String toString()
		{
			return eci + "," + stime + "," + etime;
		}
	}
}
