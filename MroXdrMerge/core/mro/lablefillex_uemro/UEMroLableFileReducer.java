package mro.lablefillex_uemro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Sample_4G;
import StructData.NC_LTE;
import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
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
import mro.lablefill.CellTimeKey;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStat.UserActTime;
import mro.lablefill.UserActStat.UserCell;
import mro.lablefill.UserActStat.UserCellAll;
import mro.lablefill.UserActStatMng;
import mro.lablefill.XdrLable;
import mro.lablefill.XdrLableMng;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.MrLocation;
import xdr.lablefill.ResultHelper;

public class UEMroLableFileReducer
{

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

		private Context context;
		protected static final Log LOG = LogFactory.getLog(MroDataFileReducers.class);
		private final int TimeSpan = 600;// 10分钟间隔
		private String[] strs;

		private StringBuilder tmSb = new StringBuilder();

		private StatDeal statDeal;
		private StatDeal_DT statDeal_DT;
		private StatDeal_CQT statDeal_CQT;
		private XdrLableMng xdrLableMng;
		private UserActStatMng userActStatMng;

		private Map<String, StructData.NC_LTE> ncLteMap = new HashMap<String, StructData.NC_LTE>();
		private Map<String, StructData.NC_GSM> ncGsmMap = new HashMap<String, StructData.NC_GSM>();
		private Map<String, StructData.NC_TDS> ncTdsMap = new HashMap<String, StructData.NC_TDS>();

		private ParseItem parseItem;
		private DataAdapterReader dataAdapterReader;

		private UserProp userProp;
		private UserLocer userLocer;
		private long xdr_eci = 0;// 记录xdr的eci
		private long xdrtime = 0;// 记录xdr的time
		private FigureFixedOutput figureMroFix;// 指纹库定位结果输出

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			figureMroFix = new FigureFixedOutput(context, conf);
			figureMroFix.setup();

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

			this.context = context;

			// mos = new
			// org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<NullWritable,
			// Text>(context);
			// 初始化输出控制

			if (!path_sample.contains(":"))
			{
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			}
			else
			{
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			}

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

			mosMng.init();
			////////////////////

			// 初始化小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
				throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
			}

			////////////////////
			statDeal = new StatDeal(mosMng);
			statDeal_DT = new StatDeal_DT(mosMng);
			statDeal_CQT = new StatDeal_CQT(mosMng);
			xdrLableMng = new XdrLableMng();
			userActStatMng = new UserActStatMng();

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
				outUserData();
				outAllData();
				figureMroFix.cleanup();
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
			else
			{
				// mrodata values
				// 获取tmCellID+TimeStamp+MmeUeS1apId+EventType值，////////////////////////////////////////////////////////////////////////////////////////////////////////

				LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
				if (cellInfo == null)// 此mr没有存在工参与之对应，抛掉
				{
					LOGHelper.GetLogger().writeLog(LogType.info, "gongcan no eci:" + key.getEci());
					// return;
				}

				parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-UE");
				if (parseItem == null)
				{
					throw new IOException("parse item do not get.");
				}
				dataAdapterReader = new DataAdapterReader(parseItem);

				List<SIGNAL_MR_All> mroItemList = new ArrayList<SIGNAL_MR_All>();
				List<SIGNAL_MR_All> xdrNoFixedItemList = new ArrayList<SIGNAL_MR_All>();
				int curTimeSpan = 0;
				for (Text value : values)
				{
					strs = value.toString().split(parseItem.getSplitMark(), -1);
					dataAdapterReader.readData(strs);

					StructData.SIGNAL_MR_All mrData = new StructData.SIGNAL_MR_All();
					try
					{
						mrData.FillData(dataAdapterReader);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "mro filldata err :  " + value.toString());
					}

					int time = mrData.tsc.beginTime;
					if (curTimeSpan == 0)
					{
						curTimeSpan = time / TimeSpan * TimeSpan;
					}

					if (mrData.tsc.MmeUeS1apId <= 0 || mrData.tsc.Eci <= 0 || mrData.tsc.beginTime <= 0)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "mro data err :  " + value.toString());
						continue;
					}
					// 附上地市id
					mrData.tsc.cityID = cellInfo.cityid;
					boolean xdrFixFlag = false;
					if (key.getEci() == xdr_eci && key.getTimeSpan() == xdrtime)
					{
						xdrFixFlag = xdrLableMng.dealMroData(mrData);
					}

					if (xdrFixFlag)
					{
						mroItemList.add(mrData);
					}
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc) || MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc2))// 所有的mr都保存
					{
						xdrNoFixedItemList.add(mrData);
					}
				}

				// 定位代码
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc))
				{
					// 注意mr的分割符，修改配置文件,
					// #FROMAT_MARK:UserLoc
					// #COMPILE_MARK:Home,UserLoc
					// FigureConfigPath SimuLocConfigPath
					// hadoop jar aaaloc.jar NULL 01_170104 NULL NULL NULL
					// MROLOC_NEW
					if (currEci == 0 || currEci != key.getEci())
					{
						currEci = key.getEci();
						userProp = new UserProp();
					}

					if (userProp != null)
					{
						try
						{
							userProp.DoWork(key.getEci(), xdrNoFixedItemList);
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "userProp.DoWork error ", e);
						}
					}
				}
				else if (MainModel.GetInstance().getCompile().Assert(CompileMark.UserLoc2))
				{
					// 注意mr的分割符，修改配置文件,
					// #FROMAT_MARK:UserLoc2
					// #COMPILE_MARK:Home,UserLoc2
					// SimuLocConfigPath
					// hadoop jar aaaloc.jar NULL 01_170104 NULL NULL NULL
					// MROLOC_NEW
					if (currEci == 0 || currEci != key.getEci())
					{
						currEci = key.getEci();
						userLocer = new UserLocer();
					}

					if (userLocer != null)
					{
						try
						{
							userLocer.DoWork(1, xdrNoFixedItemList, mosMng);
						}
						catch (Exception e)
						{
							LOGHelper.GetLogger().writeLog(LogType.error, "userProp.DoWork error ", e);
						}
					}
				}
				// xdr定位输出
				outDealingData(mroItemList);
				mroItemList.clear();
				// 指纹库定位输出
				figureMroFix.FigureMroItemList = xdrNoFixedItemList;
				figureMroFix.outDealingData();
			}
		}

		private long currEci = 0;

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

		}

		// 将会吐出用户最后所有数据
		private void outUserData()
		{
			// 生成用户栅格块索引表
			// for (HourDataDeal gridTimeDeal :
			// gridDeal.getHourDataDealMap().values())
			// {
			// for (ImsiBlockKey imsiBlockKey :
			// gridTimeDeal.getImsiBlockSampleMap().keySet())
			// {
			// try
			// {
			// curText.set(ResultHelper.getPutImsiSampleIndex(imsiBlockKey.getImsi(),
			// gridTimeDeal.getTimeSpan(), imsiBlockKey.getBlock()));
			// mosMng.write("imsisampleindex", NullWritable.get(), curText);
			// }
			// catch (Exception e)
			// {
			// writeLog("insert data error " + e.getMessage());
			// }
			// }
			// gridTimeDeal.getImsiBlockSampleMap().clear();
			// }

			// dt
			// 生成用户栅格块索引表
			// for (HourDataDeal gridTimeDeal :
			// gridDeal_DT.getHourDataDealMap().values())
			// {
			// for (ImsiBlockKey imsiBlockKey :
			// gridTimeDeal.getImsiBlockSampleMap().keySet())
			// {
			// try
			// {
			// curText.set(ResultHelper.getPutImsiSampleIndex(imsiBlockKey.getImsi(),
			// gridTimeDeal.getTimeSpan(), imsiBlockKey.getBlock()));
			// mosMng.write("sampleindexdt", NullWritable.get(), curText);
			// }
			// catch (Exception e)
			// {
			// writeLog("insert data error " + e.getMessage());
			// }
			// }
			// gridTimeDeal.getImsiBlockSampleMap().clear();
			// }

			// cqt
			// 生成用户栅格块索引表
			// for (HourDataDeal gridTimeDeal :
			// gridDeal_CQT.getHourDataDealMap().values())
			// {
			// for (ImsiBlockKey imsiBlockKey :
			// gridTimeDeal.getImsiBlockSampleMap().keySet())
			// {
			// try
			// {
			// curText.set(ResultHelper.getPutImsiSampleIndex(imsiBlockKey.getImsi(),
			// gridTimeDeal.getTimeSpan(), imsiBlockKey.getBlock()));
			// mosMng.write("sampleindexcqt", NullWritable.get(), curText);
			// }
			// catch (Exception e)
			// {
			// writeLog("insert data error " + e.getMessage());
			// }
			// }
			// gridTimeDeal.getImsiBlockSampleMap().clear();
			// }
			//

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

				statMro(sample, data);
				statKpi(sample);
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
			tsam.ispeed = tTemp.ispeed;
			tsam.imode = tTemp.imode;
			tsam.ilatitude = tTemp.tsc.latitude;
			tsam.ilongitude = tTemp.tsc.longitude;
			tsam.simuLatitude = tTemp.simuLatitude;
			tsam.simuLongitude = tTemp.simuLongitude;
			tsam.testType = tTemp.testType;

			// sam
			// tsam.ispeed = (tTemp.tsc.beginTimems >> 16); // 标识经纬度来源
			tsam.cityID = tTemp.tsc.cityID;
			tsam.itime = tTemp.tsc.beginTime;
			tsam.wtimems = (short) (tTemp.tsc.beginTimems);
			tsam.ilongitude = tTemp.tsc.longitude;
			tsam.ilatitude = tTemp.tsc.latitude;
			tsam.IMSI = tTemp.tsc.IMSI;
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
			tsam.LteSceNBRxTxTimeDiff = tTemp.tsc.LteSceNBRxTxTimeDiff;

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

			calJamType(tsam);

			// output to hbase
			try
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.OutAllSample))
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("mrosample", NullWritable.get(), curText);
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

		private void statLteNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
		{
			if (item.LteNcRSRP != StaticConfig.Int_Abnormal && item.LteNcEarfcn > 0 && item.LteNcPci > 0)
			{
				String key = item.LteNcEarfcn + "_" + item.LteNcPci;

				StructData.NC_LTE data = ncLteMap.get(key);
				if (data == null)
				{
					data = new StructData.NC_LTE();
					data.LteNcEarfcn = item.LteNcEarfcn;
					data.LteNcPci = item.LteNcPci;
					data.LteNcRSRP = item.LteNcRSRP;
					data.LteNcRSRQ = item.LteScRSRQ;

					ncLteMap.put(key, data);
				}
				else
				{
					if (item.LteNcRSRP > data.LteNcRSRP)
					{
						data.LteNcRSRP = item.LteNcRSRP;
						data.LteNcRSRQ = item.LteNcRSRQ;
					}
				}
			}
		}

		private void statGsmNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
		{
			if (item.GsmNcellCarrierRSSI != StaticConfig.Int_Abnormal && item.GsmNcellBcch > 0 && item.GsmNcellBcc > 0)
			{
				String key = item.GsmNcellBcch + "_" + item.GsmNcellBcc;

				StructData.NC_GSM data = ncGsmMap.get(key);
				if (data == null)
				{
					data = new StructData.NC_GSM();
					data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
					data.GsmNcellBsic = item.GsmNcellBcc;
					data.GsmNcellBcch = item.GsmNcellBcch;

					ncGsmMap.put(key, data);
				}
				else
				{
					if (item.GsmNcellCarrierRSSI > data.GsmNcellCarrierRSSI)
					{
						data.GsmNcellCarrierRSSI = item.GsmNcellCarrierRSSI;
					}
				}

			}
		}

		private void statTdsNbCell(SIGNAL_MR_All mrResult, StructData.MroOrigDataMT item)
		{
			if (item.TdsPccpchRSCP != StaticConfig.Int_Abnormal && item.TdsNcellUarfcn > 0 && item.TdsCellParameterId > 0)
			{
				String key = item.TdsNcellUarfcn + "_" + item.TdsCellParameterId;

				StructData.NC_TDS data = ncTdsMap.get(key);
				if (data == null)
				{
					data = new StructData.NC_TDS();
					data.TdsPccpchRSCP = item.TdsPccpchRSCP;
					data.TdsNcellUarfcn = (short) item.TdsNcellUarfcn;
					data.TdsCellParameterId = (short) item.TdsCellParameterId;

					ncTdsMap.put(key, data);
				}
				else
				{
					if (item.TdsPccpchRSCP > data.TdsPccpchRSCP)
					{
						data.TdsPccpchRSCP = item.TdsPccpchRSCP;
					}
				}

			}
		}
	}
}
