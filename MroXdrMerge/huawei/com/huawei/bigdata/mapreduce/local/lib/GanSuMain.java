package com.huawei.bigdata.mapreduce.local.lib;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealJob;
import mergestat.MergeDataFactory;
import mergestat.MergeStatMain;
import mro.format_mt.MroFormatMTMain;
import mro.lablefill.MroLableFillMain;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.lablefill.XdrLableFillMain;
import xdr.prepare.XdrPrepareMain;

public class GanSuMain extends Configured implements Tool
{
    public static final String sysPath = System.getProperty("user.dir");
    public static HashMap<String, String> paraMap = new HashMap<String, String>();
	public static Set<String> handleFileSet = new HashSet<String>();
	
    public static final String confFile = "conf/app.properties";
    private static final String handFile = ".handle.list";
    
    //=============================================================================================
    private static Map<String, Integer> statTypeMap = new HashMap<String, Integer>();
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");

    

	@Override
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		if(conf == null)
		{
			System.out.println("this is bad config.");
		}
		else 
		{
			System.out.println("this is good config.");
		}
		
		String queueName = args[0];// network
		String statTime = args[1];// 01_151013
		String xdrDataPath_mme = args[2];
		String xdrDataPath_http = args[3];
		String xdrLocationPath = args[4];
		String statType = args.length >= 6 ? args[5] : "";

		if (xdrDataPath_mme.length() == 0 || xdrDataPath_http.length() == 0)
		{
			System.out.println("xdr data path is null, check the inputs ");
			return 0;
		}
		
		String[] statTypes = statType.split(",");
		statTypeMap.clear();
		if (statType.trim().length() > 0)
		{
			for (int i = 0; i < statTypes.length; ++i)
			{
				statTypes[i] = statTypes[i].toUpperCase();
				statTypeMap.put(statTypes[i], 0);
			}
		}

		// MainModel.GetInstance().getAppConfig().saveConfigure();
		
		System.out.println("hdfsOper config : " + conf.get("fs.defaultFS"));

		HDFSOper hdfsOper = new HDFSOper(conf);
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		String mroDataPath = MainModel.GetInstance().getAppConfig().getMroDataPath();
		String mtMroDataPath = MainModel.GetInstance().getAppConfig().getMTMroDataPath();
		String mreDataPath = MainModel.GetInstance().getAppConfig().getMreDataPath();

		System.out.println("<mroXdrMergePath>: " + mroXdrMergePath);
		System.out.println("<mroDataPath>: " + mroDataPath);
		System.out.println("<mreDataPath>: " + mreDataPath);
		System.out.println("<statType>: " + statType);

		if (checkDoStat("XDRPREPARE"))
		{
			// 小区统计
			String[] myArgs = new String[6];
			// myArgs[0] = "100";
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrDataPath_mme;
			myArgs[4] = xdrDataPath_http;
			myArgs[5] = String.format("%s/xdr_prepare/data_%s", mroXdrMergePath, statTime);

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job xdrPrepareJob = XdrPrepareMain.CreateJob(conf, myArgs);
				Date stime = new Date();

				if (!xdrPrepareJob.waitForCompletion(true))
				{
					System.out.println("xdr prepare Job error! stop run.");
					throw (new Exception("sysExit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[4], mins, timeFormat.format(stime),
						timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("XDRPrepare has bend dealed succesfully:" + myArgs[5]);
			}
		}
		
		
		if (checkDoStat("XDRLOC"))
		{
			String locPath = "";
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi))
			{
				locPath = String.format("%s/loc_fill/data_%s/location_%s", mroXdrMergePath, statTime, statTime);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
			{
				locPath = String.format("%s/xdr_prepare/data_%s/TB_LOCATION_%s", mroXdrMergePath, statTime, statTime);
			}
			else
			{
				locPath = xdrLocationPath;
			}

			// 执行xdr运算
			String[] myArgs = new String[11];
			// myArgs[0] = "3000";
			myArgs[0] = "1000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrDataPath_mme;
			myArgs[4] = xdrDataPath_http;
			myArgs[5] = String.format("/flume/23G/location/%s", "20" + statTime.substring(3));// 23g
																								// input
																								// //
																								// path
			myArgs[6] = String.format("%s/xdrcellmark", mroXdrMergePath);
			myArgs[7] = locPath;
			myArgs[8] = "";
			myArgs[9] = String.format("%s/xdr_prepare/data_%s/TB_IMSI_COUNT_%s", mroXdrMergePath, statTime, statTime);
			myArgs[10] = String.format("%s/xdr_loc/data_%s", mroXdrMergePath, statTime);
		
			String _successFile = myArgs[10] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job xdrLocJob = XdrLableFillMain.CreateJob(conf, myArgs);
				Date stime = new Date();

				if (!xdrLocJob.waitForCompletion(true))
				{
					System.out.println("xdrLocJob error! stop run.");
					throw (new Exception("sysExit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[10], mins, timeFormat.format(stime),
						timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("XDRLOC has been dealed succesfully:" + myArgs[10]);
			}
		}

		if (checkDoStat("MROFORMATMT"))
		{
			// 执行mro format
			String[] myArgs = new String[6];
			myArgs[0] = "2000";
			// myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			myArgs[4] = String.format("%1$s/%2$s", mreDataPath, statTime.substring(3, 9));
			myArgs[5] = String.format("%s/mroformat/data_%s", mroXdrMergePath, statTime);

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing))
			{
				String mroPath = "/ws/detail/mro-alcatel5/p1_day=" + "20" + statTime.substring(3, 9);
				mroPath += "," + "/ws/detail/mro-datang5/p1_day=" + "20" + statTime.substring(3, 9);
				mroPath += "," + "/ws/detail/mro-ericsson5/p1_day=" + "20" + statTime.substring(3, 9);
				mroPath += "," + "/ws/detail/mro-huawei2/p1_day=" + "20" + statTime.substring(3, 9);
				mroPath += "," + "/ws/detail/mro-huawei5/p1_day=" + "20" + statTime.substring(3, 9);
				mroPath += "," + "/ws/detail/mro-zte5/p1_day=" + "20" + statTime.substring(3, 9);

				// String mroPath = "/szmt/Data/test/mro";

				myArgs[3] = mroPath;
			}

			//myArgs[3] = "/szmt/Data/test/zx_mr";
			
			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroformatJob = MroFormatMTMain.CreateJob(conf, myArgs);

				Date stime = new Date();

				if (!mroformatJob.waitForCompletion(true))
				{
					System.out.println("mroformatJob error! stop run.");
					throw (new Exception("sysExit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime),
						timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROFORMAT has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("MROLOC"))
		{
			// 执行mro运算
			String[] myArgs = new String[6];
			myArgs[0] = "3000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/xdr_loc/data_%2$s/XDR_LOCATION_%2$s", mroXdrMergePath, statTime);
			myArgs[4] = String.format("%1$s/mroformat/data_%2$s/mroformat_%2$s", mroXdrMergePath, statTime);
			myArgs[5] = String.format("%s/mro_loc/data_%s", mroXdrMergePath, statTime);

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = MroLableFillMain.CreateJob(conf, myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("sysExit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime),
						timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROLOC has bend dealed succesfully:" + myArgs[5]);
			}
		}
		
		///////////////////////////////////////////////// MERGE STAT
		///////////////////////////////////////////////// //////////////////////////////////////////////////////
		if (checkDoStat("MERGESTAT"))
		{
			// 执行xdr运算
			int pcnt = 0;
			String[] myArgs = new String[1000];
			myArgs[pcnt++] = "100";
			myArgs[pcnt++] = queueName;
			myArgs[pcnt++] = statTime;
			myArgs[pcnt++] = String.format("%s/mergestat/data_%s", mroXdrMergePath, statTime);

			///////////////////////////////////////////// input
			///////////////////////////////////////////// //////////////////////////////////////////////////

			myArgs[pcnt++] = "36";// input path count

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_SIGNAL_CELL_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_SIGNAL_CELL_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_SIGNAL_CELL_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_SIGNAL_CELL_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_SIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_SIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_SIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_SIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_SIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_SIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_SIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_SIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_DTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_DTSIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_DTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_DTSIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_DTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_DTSIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_DTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_DTSIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_2G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_3G;
			myArgs[pcnt++] = String.format(
					"%1$s/xdr_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G;
			myArgs[pcnt++] = String.format(
					"%1$s/mro_loc/data_%2$s/TB_DTSIGNAL_GRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_DTSIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G;
			myArgs[pcnt++] = String.format(
					"%1$s/mro_loc/data_%2$s/TB_CQTSIGNAL_GRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_CQTSIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_ALL_4G;
			myArgs[pcnt++] = String.format(
					"%1$s/mro_loc/data_%2$s/TB_SIGNAL_GRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_SIGNAL_GRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_4G;
			myArgs[pcnt++] = String.format(
					"%1$s/mro_loc/data_%2$s/TB_SIGNAL_CELL_%2$s,%1$s/xdr_loc/data_%2$s/TB_SIGNAL_CELL_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_4G;
			myArgs[pcnt++] = String.format(
					"%1$s/mro_loc/data_%2$s/TB_SIGNAL_CELLGRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_SIGNAL_CELLGRID_%2$s",
					mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_USERGRIDSTAT_4G;
			myArgs[pcnt++] = String.format("%1$s/xdr_loc/data_%2$s/TB_SIGNAL_GRID_USER_HOUR_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G_FREQ;
			myArgs[pcnt++] = String.format("%1$s/mro_loc/data_%2$s/TB_FREQ_DTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G_FREQ;
			myArgs[pcnt++] = String.format("%1$s/mro_loc/data_%2$s/TB_FREQ_CQTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_FREQ;
			myArgs[pcnt++] = String.format("%1$s/mro_loc/data_%2$s/TB_FREQ_SIGNAL_CELL_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_USERACT_CELL;
			myArgs[pcnt++] = String.format("%1$s/mro_loc/data_%2$s/TB_SIG_USER_BEHAVIOR_LOC_MR_%2$s", mroXdrMergePath,
					statTime);
			// -----------
			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G;
			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_SIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G;
			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_DTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G;
			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_CQTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G;
			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_SIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G;
			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_Freq_DTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G;
			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_Freq_CQTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);
			// -----------
//			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G_10;
//			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_SIGNAL_Grid10_%2$s", mroXdrMergePath,
//					statTime);
//
//			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G_10;
//			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_DTSIGNAL_Grid10_%2$s", mroXdrMergePath,
//					statTime);
//
//			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G_10;
//			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_CQTSIGNAL_Grid10_%2$s", mroXdrMergePath,
//					statTime);
//
//			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G_10;
//			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_SIGNAL_CELLGRID10_%2$s", mroXdrMergePath,
//					statTime);
//
//			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G_10;
//			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_Freq_DTSIGNAL_Grid10_%2$s", mroXdrMergePath,
//					statTime);
//
//			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G_10;
//			myArgs[pcnt++] = String.format("%1$s/figure/data_%2$s/TB_Freq_CQTSIGNAL_Grid10_%2$s", mroXdrMergePath,
//					statTime);

			///////////////////////////////////////////// output
			///////////////////////////////////////////// //////////////////////////////////////////////////

			myArgs[pcnt++] = "36";// output path count

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_2G;
			myArgs[pcnt++] = String.format("cell2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_SIGNAL_CELL_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_3G;
			myArgs[pcnt++] = String.format("cell3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_SIGNAL_CELL_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_2G;
			myArgs[pcnt++] = String.format("grid2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_SIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_3G;
			myArgs[pcnt++] = String.format("grid3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_SIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_2G;
			myArgs[pcnt++] = String.format("cellgrid2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_SIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_3G;
			myArgs[pcnt++] = String.format("cellgrid3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_SIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_2G;
			myArgs[pcnt++] = String.format("griddt2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_DTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_3G;
			myArgs[pcnt++] = String.format("griddt3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_DTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_2G;
			myArgs[pcnt++] = String.format("cellgriddt2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_DTSIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_3G;
			myArgs[pcnt++] = String.format("cellgriddt3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_DTSIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_2G;
			myArgs[pcnt++] = String.format("gridcqt2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_CQTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_3G;
			myArgs[pcnt++] = String.format("gridcqt3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_CQTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_2G;
			myArgs[pcnt++] = String.format("cellgridcqt2g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_2G_CQTSIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_3G;
			myArgs[pcnt++] = String.format("cellgridcqt3g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_3G_CQTSIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G;
			myArgs[pcnt++] = String.format("griddt4g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G;
			myArgs[pcnt++] = String.format("gridcqt4g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_CQTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_ALL_4G;
			myArgs[pcnt++] = String.format("grid4g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_GRID_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_4G;
			myArgs[pcnt++] = String.format("cell4g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_CELL_%2$s", mroXdrMergePath, statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_4G;
			myArgs[pcnt++] = String.format("cellgrid4g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_USERGRIDSTAT_4G;
			myArgs[pcnt++] = String.format("usergrid4g");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_GRID_USER_HOUR_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G_FREQ;
			myArgs[pcnt++] = String.format("griddt4gfreq");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_FREQ_DTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G_FREQ;
			myArgs[pcnt++] = String.format("gridcqt4gfreq");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_FREQ_CQTSIGNAL_GRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_CELLSTAT_FREQ;
			myArgs[pcnt++] = String.format("cellgridfreq");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_FREQ_SIGNAL_CELL_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_USERACT_CELL;
			myArgs[pcnt++] = String.format("useractcell");
			myArgs[pcnt++] = String.format("%1$s/mergestat/data_%2$s/TB_SIG_USER_BEHAVIOR_LOC_MR_%2$s", mroXdrMergePath,
					statTime);
			// --
			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G;
			myArgs[pcnt++] = String.format("figuregrid");
			myArgs[pcnt++] = String.format("%1$s/mergestat_figure/data_%2$s/TB_SIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G;
			myArgs[pcnt++] = String.format("figuredtgrid");
			myArgs[pcnt++] = String.format("%1$s/mergestat_figure/data_%2$s/TB_DTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G;
			myArgs[pcnt++] = String.format("figurecqtgrid");
			myArgs[pcnt++] = String.format("%1$s/mergestat_figure/data_%2$s/TB_CQTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G;
			myArgs[pcnt++] = String.format("figurecellgrid");
			myArgs[pcnt++] = String.format("%1$s/mergestat_figure/data_%2$s/TB_SIGNAL_CELLGRID_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G;
			myArgs[pcnt++] = String.format("figurefreqdtgrid");
			myArgs[pcnt++] = String.format("%1$s/mergestat_figure/data_%2$s/TB_Freq_DTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);

			myArgs[pcnt++] = "" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G;
			myArgs[pcnt++] = String.format("figurefreqcqtgrid");
			myArgs[pcnt++] = String.format("%1$s/mergestat_figure/data_%2$s/TB_Freq_CQTSIGNAL_Grid_%2$s", mroXdrMergePath,
					statTime);
			
			// --
			// myArgs[pcnt++] = "" +
			// MergeDataFactory.MERGETYPE_FIGURE_GRID_4G_10;
			// myArgs[pcnt++] = String.format("figuregrid10");
			// myArgs[pcnt++] =
			// String.format("%1$s/mergestat_figure/data_%2$s/TB_SIGNAL_Grid10_%2$s",
			// mroXdrMergePath,
			// statTime);
			//
			// myArgs[pcnt++] = "" +
			// MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G_10;
			// myArgs[pcnt++] = String.format("figuredtgrid10");
			// myArgs[pcnt++] =
			// String.format("%1$s/mergestat_figure/data_%2$s/TB_DTSIGNAL_Grid10_%2$s",
			// mroXdrMergePath,
			// statTime);
			//
			// myArgs[pcnt++] = "" +
			// MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G_10;
			// myArgs[pcnt++] = String.format("figurecqtgrid10");
			// myArgs[pcnt++] =
			// String.format("%1$s/mergestat_figure/data_%2$s/TB_CQTSIGNAL_Grid10_%2$s",
			// mroXdrMergePath,
			// statTime);
			//
			// myArgs[pcnt++] = "" +
			// MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G_10;
			// myArgs[pcnt++] = String.format("figurecellgrid10");
			// myArgs[pcnt++] =
			// String.format("%1$s/mergestat_figure/data_%2$s/TB_SIGNAL_CELLGRID10_%2$s",
			// mroXdrMergePath,
			// statTime);
			//
			// myArgs[pcnt++] = "" +
			// MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G_10;
			// myArgs[pcnt++] = String.format("figurefreqdtgrid10");
			// myArgs[pcnt++] =
			// String.format("%1$s/mergestat_figure/data_%2$s/TB_Freq_DTSIGNAL_Grid10_%2$s",
			// mroXdrMergePath,
			// statTime);
			//
			// myArgs[pcnt++] = "" +
			// MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G_10;
			// myArgs[pcnt++] = String.format("figurefreqcqtgrid10");
			// myArgs[pcnt++] =
			// String.format("%1$s/mergestat_figure/data_%2$s/TB_Freq_CQTSIGNAL_Grid10_%2$s",
			// mroXdrMergePath,
			// statTime);

			String[] params = new String[pcnt];
			for (int i = 0; i < pcnt; ++i)
			{
				params[i] = myArgs[i];
			}

			Job curJob = MergeStatMain.CreateJob(conf, params);

			DataDealJob dataJob = new DataDealJob(curJob, hdfsOper);
			if (!dataJob.Work())
			{
				System.out.println("mergestat job error! stop run.");
				throw (new Exception("sysExit1"));
			}

		}
		
		
		return 0;
	}
	
	private static boolean checkDoStat(String type)
	{
		if (statTypeMap.values().size() == 0)
		{
			return true;
		}
		return statTypeMap.containsKey(type) ? true : false;
	}
	
	
	public static void main(String[] args) throws Exception
	{	
		int exitCode = ToolRunner.run(new GanSuMain(), args);
		System.exit(exitCode);
	}
	
	private boolean loadProp() throws IOException {
		Properties prop = new Properties();
		InputStream in = null;
		try {
			JarFile jf = new JarFile(sysPath+"/MroXdrMerge_allstat.jar");
			in = jf.getInputStream(jf.getJarEntry(confFile));
			prop.load(in);
			Enumeration<Object> keys = prop.keys();
			String key = null;
			while (keys.hasMoreElements()) {
				key = keys.nextElement().toString();
				paraMap.put(key, prop.getProperty(key));
			}
		} catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
			return false;
		}
		return true;
	}
	
	
	
}
