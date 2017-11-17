package mro.lablefill_uemro;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
import jan.util.GisFunction;
import jan.util.LOGHelper;
import mro.lablefill.StatDeal;
import mro.lablefill.StatDeal_CQT;
import mro.lablefill.StatDeal_DT;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStat.UserActTime;
import mro.lablefill.UserActStat.UserCell;
import mro.lablefill.UserActStat.UserCellAll;
import mro.lablefill.UserActStatMng;
import mro.lablefill.XdrLable;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.MrLocation;
import xdr.lablefill.LocationItem;
import xdr.lablefill.ResultHelper;

public class MroLableFileReducer
{

	public static class MroDataFileReducer extends DataDealReducer<ImsiKey, Text, NullWritable, Text>
	{
		// private
		// org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<NullWritable,
		// Text> mos;
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

		private Context context;

		private final int TimeSpan = 600;// 10分钟间隔
		private String[] strs;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		private StringBuilder tmSb = new StringBuilder();

		private StatDeal statDeal;
		private StatDeal_DT statDeal_DT;
		private StatDeal_CQT statDeal_CQT;
		private XdrLableMng xdrLableMng;
		private UserActStatMng userActStatMng;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
			path_sample = conf.get("mastercom.mroxdrmerge.mro.locfill.path_sample");
			path_event = conf.get("mastercom.mroxdrmerge.mro.locfill.path_event");
			path_cell = conf.get("mastercom.mroxdrmerge.mro.locfill.path_cell");
			path_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfill.path_cell_freq");
			path_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfill.path_cellgrid");
			path_grid = conf.get("mastercom.mroxdrmerge.mro.locfill.path_grid");
			path_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfill.path_ImsiSampleIndex");
			path_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfill.path_ImsiEventIndex");
			path_myLog = conf.get("mastercom.mroxdrmerge.mro.locfill.path_myLog");
			path_locMore = conf.get("mastercom.mroxdrmerge.mro.locfill.path_locMore");
			path_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfill.path_mroMore");

			path_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfill.path_grid_dt");
			path_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfill.path_grid_dt_freq");
			path_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfill.path_grid_cqt");
			path_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfill.path_grid_cqt_freq");
			path_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfill.path_sample_dt");
			path_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfill.path_sample_dtex");
			path_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfill.path_sample_cqt");
			path_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfill.path_sample_index_dt");
			path_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfill.path_sample_index_cqt");

			path_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfill.path_useract_cell");

			this.context = context;

			// mos = new
			// org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<NullWritable,
			// Text>(context);
			// 初始化输出控制
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
			LOGHelper.GetLogger().writeLog(LogType.info,
					"cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
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
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
			}

			super.cleanup(context);

			mosMng.close();
		}

		@Override
		public void reduce(ImsiKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{

			if (key.getDataType() == 1)
			{
				xdrLableMng = new XdrLableMng();

				for (Text value : values)
				{
					String[] strs = value.toString().split("\\|" + "|" + "\t", -1);
					LocationItem item = new LocationItem();
					item.FillData(strs, 1);

					XdrLable xdrLable = new XdrLable();
					xdrLable.cityID = 0;
					xdrLable.eci = item.eci;
					xdrLable.s1apid = 0;
					xdrLable.itime = item.itime;
					xdrLable.imsi = item.imsi;
					xdrLable.ilongtude = item.longitude;
					xdrLable.ilatitude = item.latitude;

					xdrLable.testType = StaticConfig.TestType_DT;
					xdrLable.location = item.location;
					xdrLable.dist = 100;
					xdrLable.radius = item.radius;
					xdrLable.loctp = item.loctp;
					xdrLable.indoor = 0;
					xdrLable.networktype = "LTE";
					xdrLable.lable = "high";

					xdrLable.longitudeGL = item.longitude;
					xdrLable.latitudeGL = item.latitude;
					xdrLable.testTypeGL = StaticConfig.TestType_DT;

					xdrLable.locationGL = item.location;
					xdrLable.distGL = 100;
					xdrLable.radiusGL = item.radius;
					xdrLable.loctpGL = item.loctp;
					xdrLable.indoorGL = 0;
					xdrLable.lableGL = "high";

					xdrLable.serviceType = 0;
					xdrLable.subServiceType = 0;

					xdrLable.moveDirect = -1;
					xdrLable.loctimeGL = item.locTime;
					xdrLable.host = "";
					xdrLable.wifiName = "";

					try
					{
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

				List<SIGNAL_MR_All> mroItemList = new ArrayList<SIGNAL_MR_All>();
				int curTimeSpan = 0;
				Text value;
				String[] strs;
				while (values.iterator().hasNext())
				{
					value = values.iterator().next();

					SIGNAL_MR_All mroItem = new SIGNAL_MR_All();
					try
					{
						if (MainModel.GetInstance().getCompile().Assert(CompileMark.ShenZhen))
						{
							strs = value.toString().split(",", -1);
							mroItem.FillData_UEMro_ShenZhen(new Object[] { strs, 0 });
						}
						else
						{
							strs = value.toString().split(StaticConfig.DataSliper2 + "|" + "\\|" + "|" + "\t", -1);
							mroItem.FillData_UEMro(new Object[] { strs, 0 });
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "SIGNAL_MR_All.FillData error ", e);
						continue;
					}

					if (curTimeSpan == 0)
					{
						curTimeSpan = mroItem.tsc.beginTime / TimeSpan * TimeSpan;
					}

					if (mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "mro format err :  " + value);
						continue;
					}

					// 附上地市id
					LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(mroItem.tsc.Eci);
					if (cellInfo != null)
					{
						mroItem.tsc.cityID = cellInfo.cityid;
					}

					xdrLableMng.dealMroData(mroItem);
					mroItemList.add(mroItem);

				}
				outDealingData(mroItemList);
				mroItemList.clear();
			}

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
					if (data.tsc.longitude > 0 && data.tsc.latitude > 0 && lteCellInfo.ilongitude > 0
							&& lteCellInfo.ilatitude > 0)
					{
						dist = (int) GisFunction.GetDistance(data.tsc.longitude, data.tsc.latitude,
								lteCellInfo.ilongitude, lteCellInfo.ilatitude);
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
			// sam
			tsam.ispeed = (tTemp.tsc.beginTimems >> 16); // 标识经纬度来源
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

				// 吐出关联的中间结果
				// tmSb.delete(0, tmSb.length());
				// tmSb.append(tsam.Eci + "_" + tsam.MmeUeS1apId + "_" +
				// tsam.itime);
				// tmSb.append("\t");
				// tmSb.append(tsam.Earfcn);
				// tmSb.append("_");
				// tmSb.append(tsam.LteScPci);
				// tmSb.append("_");
				// tmSb.append(tsam.LteScRSRP);
				// tmSb.append("_");
				// tmSb.append(tsam.IMSI);
				// tmSb.append("_");
				// tmSb.append(tsam.ilongitude);
				// tmSb.append("_");
				// tmSb.append(tsam.ilatitude);

				// curText.set(tmSb.toString());
				// mosMng.write("mroMore", NullWritable.get(), curText);
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

	}

}
