package xdr.lablefill.by23g;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Event;
import StructData.DT_Sample_2G;
import StructData.DT_Sample_3G;
import StructData.ELocationMark;
import StructData.ELocationType;
import StructData.SIGNAL_LOC;
import StructData.SIGNAL_XDR_2G;
import StructData.SIGNAL_XDR_3G;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.GsmCellInfo;
import cellconfig.TdCellInfo;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.GisFunction;
import jan.util.GisLocater;
import jan.util.GisPos;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;
import xdr.lablefill.LabelDeal;
import xdr.lablefill.ResultHelper;
import xdr.lablefill.TestTypeDeal;

public class XdrLableFileReducer
{

	public static class XdrDataFileReducer extends DataDealReducer<ImsiTimeKey, Text, NullWritable, Text>
	{
		protected static final Log LOG = LogFactory.getLog(XdrDataFileReducer.class);

		private MultiOutputMng<NullWritable, Text> mosMng;

		public MultiOutputMng<NullWritable, Text> getMosMng()
		{
			return mosMng;
		}

		private Text curText = new Text();

		private String path_event_dt_2g;
		private String path_event_dt_3g;
		private String path_event_dtex_2g;
		private String path_event_dtex_3g;
		private String path_event_cqt_2g;
		private String path_event_cqt_3g;
		private String path_event_err_23g;

		private String path_grid_2g;
		private String path_grid_3g;
		private String path_grid_dt_2g;
		private String path_grid_dt_3g;
		private String path_grid_cqt_2g;
		private String path_grid_cqt_3g;

		private String path_cellgrid_2g;
		private String path_cellgrid_3g;
		private String path_cellgrid_dt_2g;
		private String path_cellgrid_dt_3g;
		private String path_cellgrid_cqt_2g;
		private String path_cellgrid_cqt_3g;

		private String path_cell_2g;
		private String path_cell_3g;

		private Context context;

		private final int TimeSpan = 600;// 10分钟间隔
		private String[] strs;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		private long curImsi;
		private LabelDeal labelDeal;
		private List<SIGNAL_LOC> userXdrList;
		private StatDeal statDeal;
		private StatDeal_DT statDeal_DT;
		private StatDeal_CQT statDeal_CQT;

		private int eciErrCount;
		private int locErrCount;
		private int radiusErrCount;
		private int formatErrCount;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			Configuration conf = context.getConfiguration();
			path_event_dt_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dt_2g");
			path_event_dt_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dt_3g");
			path_event_dtex_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex_2g");
			path_event_dtex_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex_3g");
			path_event_cqt_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt_2g");
			path_event_cqt_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt_3g");
			path_event_err_23g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_event_err_23g");

			path_grid_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_2g");
			path_grid_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_3g");
			path_grid_dt_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt_2g");
			path_grid_dt_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt_3g");
			path_grid_cqt_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt_2g");
			path_grid_cqt_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt_3g");

			path_cellgrid_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_2g");
			path_cellgrid_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_3g");
			path_cellgrid_dt_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_dt_2g");
			path_cellgrid_dt_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_dt_3g");
			path_cellgrid_cqt_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_cqt_2g");
			path_cellgrid_cqt_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_cqt_3g");

			path_cell_2g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cell_2g");
			path_cell_3g = conf.get("mastercom.mroxdrmerge.xdr.locfill.path_cell_3g");

			this.context = context;

			// mos = new
			// org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<NullWritable,
			// Text>(context);
			// 初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.SetOutputPath("eventdt2g", path_event_dt_2g);
			mosMng.SetOutputPath("eventdt3g", path_event_dt_3g);
			mosMng.SetOutputPath("eventdtex2g", path_event_dtex_2g);
			mosMng.SetOutputPath("eventdtex3g", path_event_dtex_3g);
			mosMng.SetOutputPath("eventcqt2g", path_event_cqt_2g);
			mosMng.SetOutputPath("eventcqt3g", path_event_cqt_3g);
			mosMng.SetOutputPath("eventerr23g", path_event_err_23g);

			mosMng.SetOutputPath("grid2g", path_grid_2g);
			mosMng.SetOutputPath("grid3g", path_grid_3g);
			mosMng.SetOutputPath("griddt2g", path_grid_dt_2g);
			mosMng.SetOutputPath("griddt3g", path_grid_dt_3g);
			mosMng.SetOutputPath("gridcqt2g", path_grid_cqt_2g);
			mosMng.SetOutputPath("gridcqt3g", path_grid_cqt_3g);

			mosMng.SetOutputPath("cellgrid2g", path_cellgrid_2g);
			mosMng.SetOutputPath("cellgrid3g", path_cellgrid_3g);
			mosMng.SetOutputPath("cellgriddt2g", path_cellgrid_dt_2g);
			mosMng.SetOutputPath("cellgriddt3g", path_cellgrid_dt_3g);
			mosMng.SetOutputPath("cellgridcqt2g", path_cellgrid_cqt_2g);
			mosMng.SetOutputPath("cellgridcqt3g", path_cellgrid_cqt_3g);

			mosMng.SetOutputPath("cell2g", path_cell_2g);
			mosMng.SetOutputPath("cell3g", path_cell_3g);

			mosMng.init();
			////////////////////

			// 初始化lte小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "ltecell init error 请检查！");
				throw (new IOException("ltecell init error 请检查！"));
			}

			// 初始化gsm小区的信息
			if (!CellConfig.GetInstance().loadGsmCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "gsmcell init error 请检查！");
				throw (new IOException("gsmcell init error 请检查！"));
			}

			// 初始化tdscdma小区的信息
			if (!CellConfig.GetInstance().loadTdCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "tdcell init error 请检查！");
				throw (new IOException("tdcell init error 请检查！"));
			}

			////////////////////

			curImsi = 0;
			eciErrCount = 0;
			locErrCount = 0;
			radiusErrCount = 0;
			formatErrCount = 0;

			userXdrList = new ArrayList<SIGNAL_LOC>();
			statDeal = new StatDeal(mosMng);
			statDeal_DT = new StatDeal_DT(mosMng);
			statDeal_CQT = new StatDeal_CQT(mosMng);

			// 打印状态日志
			LOGHelper.GetLogger().writeLog(LogType.info,
					"ltecell init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
			LOGHelper.GetLogger().writeLog(LogType.info,
					"gsmcell init count is : " + CellConfig.GetInstance().getGsmCellInfoMap().size());
			LOGHelper.GetLogger().writeLog(LogType.info,
					"tdcell init count is : " + CellConfig.GetInstance().getTDCellInfoMap().size());

		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "error eci count is :" + eciErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "format error count is :" + formatErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "error location count is :" + locErrCount);
			LOGHelper.GetLogger().writeLog(LogType.info, "error radius count is :" + radiusErrCount);

			super.cleanup(context);

			outAllData();
			mosMng.close();
		}

		public void reduce(ImsiTimeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			try
			{
				if (curImsi != key.getImsi())
				{
					// writeLog(LogType.debug, "user xdr count : " + curImsi + "
					// " + userXdrList.size());

					outUserData();

					userXdrList = new ArrayList<SIGNAL_LOC>();
					curImsi = key.getImsi();
					labelDeal = new LabelDeal(curImsi, mosMng);

				}

				List<SIGNAL_LOC> xdrLocList = new ArrayList<SIGNAL_LOC>();
				int curTimeSpan = 0;
				Text value;
				String[] strs;
				LocInfoItem locInfoItem = new LocInfoItem();

				while (values.iterator().hasNext())
				{
					value = values.iterator().next();

					strs = value.toString().split("\t", -1);

					try
					{
						if (!locInfoItem.FillData(strs, 0))
						{
							continue;
						}

						// dd司机判断
						if (locInfoItem.location == 3)
						{
							labelDeal.setDDDriver(true);
							LOGHelper.GetLogger().writeLog(LogType.info, "find dd driver user : " + value.toString());
						}

						SIGNAL_LOC locItem = null;

						if (locInfoItem.networktype == 1)// gsm
						{
							SIGNAL_XDR_2G xdrItem = new SIGNAL_XDR_2G();
							locItem = xdrItem;

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

							// =============================采样点有效性判断=============================================

							if (xdrItem.imsi <= 0 || xdrItem.lac <= 0 || xdrItem.ci <= 0 || xdrItem.stime <= 0)
							{
								continue;
							}

							GsmCellInfo cellInfo = CellConfig.GetInstance().getGsmCell(xdrItem.lac, xdrItem.ci);
							int maxRadius = 6000;
							if (cellInfo != null)
							{
								if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && cellInfo.ilongitude > 0
										&& cellInfo.ilatitude > 0)
								{
									xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude,
											cellInfo.ilongitude, cellInfo.ilatitude);
								}

								maxRadius = Math.min(maxRadius, 5 * cellInfo.radius);
								maxRadius = Math.max(maxRadius, 1500);

								xdrItem.cityID = cellInfo.cityid;
							}

							if (xdrItem.cityID < 0)
							{
								curText.set(value.toString() + "\t" + "lac ci 找不到配置");
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								eciErrCount++;
								continue;
							}

							if (xdrItem.dist >= maxRadius && xdrItem.longitude > 0)
							{
								curText.set(value.toString() + "\t" + "小区距离采样点过远：" + xdrItem.lac + "_" + xdrItem.ci
										+ " 距离: " + xdrItem.dist);
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								locErrCount++;
								continue;
							}

							if (xdrItem.dist < 0 && xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "小区经纬度配置缺失");
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								locErrCount++;
								continue;
							}

							// 如果是location > 6 属于不可预知的情况,去掉
							if (xdrItem.location > 6)
							{
								if (xdrItem.longitude > 0)
								{
									curText.set(xdrItem.valStr + "\t" + "location不在识别范围内");
									mosMng.write("eventerr23g", NullWritable.get(), curText);
								}

								continue;
							}

							if (xdrItem.longitude > 0)
							{

								// 如果location 是 0，1 那么将经纬度抹掉
								if (xdrItem.location >= 0 && xdrItem.location <= 1)
								{
									curText.set(xdrItem.valStr + "\t" + "location为01不予分析，忽略");
									mosMng.write("eventerr23g", NullWritable.get(), curText);
								}
								else if (xdrItem.location == ELocationMark.BaiDuSDK.getValue())
								{
									if (xdrItem.loctp.length() == 0)
									{
										curText.set(xdrItem.valStr + "\t" + "百度地图的loctp未知");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
									else if (xdrItem.loctp.equals(ELocationType.Cell.getName()))
									{
										curText.set(xdrItem.valStr + "\t" + "百度地图的小区定位不可用");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
									else if (xdrItem.radius > 100)
									{
										curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
										radiusErrCount++;
									}
								}
								else if (xdrItem.location == ELocationMark.GaoDeSDK.getValue())
								{
									if (!xdrItem.loctp.equals("14") && !xdrItem.loctp.equals("24")
											&& !xdrItem.loctp.equals("5")
											&& !xdrItem.loctp.equals(ELocationType.Wifi.getName()))
									{
										curText.set(xdrItem.valStr + "\t" + "高德地图的loctp未知");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
									else if (xdrItem.radius > 100)
									{
										curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
										radiusErrCount++;
									}
								}
								// 腾讯地图loctp需要转换
								else if (xdrItem.location == ELocationMark.TengXunSDK.getValue())
								{
									if (xdrItem.radius > 20)
									{
										// 腾讯定位精度>20的，都没有参考意义
										curText.set(xdrItem.valStr + "\t" + "腾讯地图的定位经度不符合要求，忽略");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
								}
							}

							// =============================采样点有效性判断=============================================

						}
						else if (locInfoItem.networktype == 2)// td
						{
							SIGNAL_XDR_3G xdrItem = new SIGNAL_XDR_3G();
							locItem = xdrItem;

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

							// =============================采样点有效性判断=============================================

							if (xdrItem.imsi <= 0 || xdrItem.lac <= 0 || xdrItem.ci <= 0 || xdrItem.stime <= 0)
							{
								continue;
							}

							TdCellInfo cellInfo = CellConfig.GetInstance().getTdCell(xdrItem.lac, xdrItem.ci);
							int maxRadius = 6000;
							if (cellInfo != null)
							{
								if (xdrItem.longitude > 0 && xdrItem.latitude > 0 && cellInfo.ilongitude > 0
										&& cellInfo.ilatitude > 0)
								{
									xdrItem.dist = (long) GisFunction.GetDistance(xdrItem.longitude, xdrItem.latitude,
											cellInfo.ilongitude, cellInfo.ilatitude);
								}

								maxRadius = Math.min(maxRadius, 5 * cellInfo.radius);
								maxRadius = Math.max(maxRadius, 1500);

								xdrItem.cityID = cellInfo.cityid;
							}

							if (xdrItem.cityID < 0)
							{
								curText.set(value.toString() + "\t" + "lac ci 找不到配置");
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								eciErrCount++;
								continue;
							}

							if (xdrItem.dist >= maxRadius && xdrItem.longitude > 0)
							{
								curText.set(value.toString() + "\t" + "小区距离采样点过远：" + xdrItem.lac + "_" + xdrItem.ci
										+ " 距离: " + xdrItem.dist);
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								locErrCount++;
								continue;
							}

							if (xdrItem.dist < 0 && xdrItem.longitude > 0)
							{
								curText.set(xdrItem.valStr + "\t" + "小区经纬度配置缺失");
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								locErrCount++;
								continue;
							}

							// 如果是location > 6 属于不可预知的情况,去掉
							if (xdrItem.location > 6)
							{
								curText.set(value.toString() + "\t" + "location不在识别范围内：" + xdrItem.location);
								mosMng.write("eventerr23g", NullWritable.get(), curText);
								continue;
							}

							if (xdrItem.longitude > 0)
							{
								// 如果location 是 0，1 那么将经纬度抹掉
								if (xdrItem.location >= 0 && xdrItem.location <= 1)
								{
									curText.set(xdrItem.valStr + "\t" + "location为01不予分析，忽略");
									mosMng.write("eventerr23g", NullWritable.get(), curText);
								}
								else if (xdrItem.location == ELocationMark.BaiDuSDK.getValue())
								{
									if (xdrItem.loctp.length() == 0)
									{
										curText.set(xdrItem.valStr + "\t" + "百度地图的loctp未知");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
									else if (xdrItem.loctp.equals(ELocationType.Cell.getName()))
									{
										curText.set(xdrItem.valStr + "\t" + "百度地图的小区定位不可用");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
									else if (xdrItem.radius > 100)
									{
										curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
										radiusErrCount++;
									}
								}
								else if (xdrItem.location == ELocationMark.GaoDeSDK.getValue())
								{
									if (!xdrItem.loctp.equals("14") && !xdrItem.loctp.equals("24")
											&& !xdrItem.loctp.equals("5")
											&& !xdrItem.loctp.equals(ELocationType.Wifi.getName()))
									{
										curText.set(xdrItem.valStr + "\t" + "高德地图的loctp未知");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
									else if (xdrItem.radius > 100)
									{
										curText.set(xdrItem.valStr + "\t" + "有经纬度，精度不符合要求");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
										radiusErrCount++;
									}
								}
								// 腾讯地图loctp需要转换
								else if (xdrItem.location == ELocationMark.TengXunSDK.getValue())
								{
									if (xdrItem.radius > 20)
									{
										// 腾讯定位精度>20的，都没有参考意义
										curText.set(xdrItem.valStr + "\t" + "腾讯地图的定位经度不符合要求，忽略");
										mosMng.write("eventerr23g", NullWritable.get(), curText);
									}
								}
							}

							// =============================采样点有效性判断=============================================

						}

						if (locItem != null)
						{
							if (curTimeSpan == 0)
							{
								curTimeSpan = locItem.stime / TimeSpan * TimeSpan;
							}

							xdrLocList.add(locItem);
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error,
								"locationItem.FillData error : " + value.toString(), e);
						formatErrCount++;
						continue;
					}

				}

				labelDeal.deal(xdrLocList);
				outDealingData(xdrLocList);
				xdrLocList.clear();

			}
			catch (Exception e)
			{
				throw e;
				// TODO: handle exception
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

			TestTypeDeal testTypeDeal = new TestTypeDeal(curImsi, labelDeal.IsDDDriver(),
					labelDeal.getUserHomeCellMap());

			testTypeDeal.deal(xdrItemList);

			dealSample(xdrItemList);
		}

		// 将会吐出用户最后所有数据
		private void outUserData()
		{
			dealUserData(userXdrList);

			statDeal.outDealingData();
			statDeal_DT.outDealingData();
			statDeal_CQT.outDealingData();
		}

		// 将吐出程序结束前滞留的数据
		private void outAllData()
		{
			outUserData();

			statDeal.outAllData();
			;
			statDeal_DT.outAllData();
			;
			statDeal_CQT.outAllData();
		}

		private void dealSample(List<SIGNAL_LOC> xdrList)
		{
			DT_Sample_2G sample_2g = new DT_Sample_2G();
			DT_Sample_3G sample_3g = new DT_Sample_3G();

			for (SIGNAL_LOC data : xdrList)
			{
				if (data.testType == StaticConfig.TestType_ERROR)
				{
					continue;
				}

				// 如果是2g数据，就按照2g的数据处理
				if (data instanceof SIGNAL_XDR_2G)
				{
					sample_2g.Clear();

					statXdr_2G(sample_2g, (SIGNAL_XDR_2G) data);

					statDeal.dealSample(sample_2g);
					if (data.testType == StaticConfig.TestType_DT)
					{
						statDeal_DT.dealSample(sample_2g);
					}
					else if (data.testType == StaticConfig.TestType_CQT)
					{
						statDeal_CQT.dealSample(sample_2g);
					}
				}

				// 如果是3g数据，就按照3g的数据处理
				else if (data instanceof SIGNAL_XDR_3G)
				{
					sample_3g.Clear();

					statXdr_3G(sample_3g, (SIGNAL_XDR_3G) data);

					statDeal.dealSample(sample_3g);
					statDeal_DT.dealSample(sample_3g);
					statDeal_CQT.dealSample(sample_3g);

				}

			}

		}

		private void statXdr_2G(DT_Sample_2G tsam, SIGNAL_XDR_2G tTemp)
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

			outEvent_2G(tEvt);

		}

		private void outEvent_2G(DT_Event tEvt)
		{
			try
			{
				// curText.set(ResultHelper.getPutLteEvent(tEvt));
				// mosMng.write("xdrevent", NullWritable.get(), curText);

				if (tEvt.testType == StaticConfig.TestType_DT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdt2g", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_DT_EX)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdtex2g", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_CQT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventcqt2g", NullWritable.get(), curText);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "outEvent error ", e);
			}

		}

		private void statXdr_3G(DT_Sample_3G tsam, SIGNAL_XDR_3G tTemp)
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

			outEvent_3G(tEvt);

		}

		private void outEvent_3G(DT_Event tEvt)
		{
			try
			{
				// curText.set(ResultHelper.getPutLteEvent(tEvt));
				// mosMng.write("xdrevent", NullWritable.get(), curText);

				if (tEvt.testType == StaticConfig.TestType_DT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdt3g", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_DT_EX)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventdtex3g", NullWritable.get(), curText);
				}
				else if (tEvt.testType == StaticConfig.TestType_CQT)
				{
					curText.set(ResultHelper.getPutLteEvent(tEvt));
					mosMng.write("eventcqt3g", NullWritable.get(), curText);
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

	}

}
