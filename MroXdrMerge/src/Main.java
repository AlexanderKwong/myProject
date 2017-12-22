import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import com.huawei.bigdata.mapreduce.local.lib.SecurityUtils;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.TmpFileFilter;
import loc.imsifill.LocImsiFillMain;
import localsimu.adjust.eciFigure.AdjustFigureMain;
import localsimu.cellgrid_merge.MergeCellGridMain;
import localsimu.eci_cellgrid_table.CreateEciCellGridTableMain;
import localsimu.grid_eci_table.CreateGridEciTableMain;
import mro.format_mt.MroFormatMTMain;
import mro.lablefill.MroLableFillMain;
import mro.lablefill_uemro.UEMroLableFillMain;
import mro.lablefill_xdr_figure.MroFgLableFillMains;
import mro.lablefillex.MroLableFillMains;
//import logic.mapred.mroLoc.MroLableFillMains;
import mro.lablefillex_uemro.UEMroLableFillExMain;
import mro.villagestat.VillageStatMain;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.lablefill.XdrLableFillMain;
import xdr.lablefill.by23g.XdrLableFill23GMain;
import xdr.locallex.LocAllEXMain;
import xdr.prepare.XdrPrepareMain;

public class Main
{
	public static final int XDRPREPARE = 1;
	public static final int XDRLOC = 2;
	public static final int MROFORMATMT = 3;
	public static final int MROLOC = 4;
	public static final int MROVILLAGESTAT = 5;
	public static final int GRIDMERGE_CQT = 6;
	public static final int GRIDMERGE_DT = 7;
	public static final int GRIDMERGE = 8;
	public static final int CELLSTAT = 9;
	public static final int CELLGRIDSTAT = 10;
	public static final int XDRLOC23G = 11;
	public static final int MERGESTAT1 = 12;
	public static final int LOCFILL = 13;
	public static final int MROLOCUE = 14;
	public static final int MROLOCFG = 15;
	public static final int CELLGRIDMERGE = 16;
	public static final int GRIDECITABLESTAT = 17;
	public static final int ECICELLGRIDSTAT = 18;
	public static final int ADJUSTFG = 19;
	public static final int FGOTTSTAT = 20;
	public static final int MERGESTAT2 = 21;
	public static final int MERGESTAT3 = 22;
	public static final int MERGESTAT4 = 23;
	private static Map<String, Integer> statTypeMap = new HashMap<String, Integer>();
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmm");

	public static void main(String[] args) throws Exception
	{
		if (args.length < 6)
		{
			System.out.println("args num is not right. " + args.length);
			return;
		}

		String mmeKeyWord = "";
		String mmeFilter = "";
		String httpKeyWord = "";
		String httpFilter = "";
		String mrKeyWord = "";
		String mrFilter = "";
		String locKeyWord = "";
		String locFilter = "";

		if (args.length >= 14)
		{
			if (!args[6].toUpperCase().equals("NULL") && !args[7].toUpperCase().equals("NULL"))
			{
				mmeKeyWord = args[6];
				mmeFilter = args[7];
			}
			if (!args[8].toUpperCase().equals("NULL") && !args[9].toUpperCase().equals("NULL"))
			{
				httpKeyWord = args[8];
				httpFilter = args[9];
			}
			if (!args[10].toUpperCase().equals("NULL") && !args[11].toUpperCase().equals("NULL"))
			{
				mrKeyWord = args[10];
				mrFilter = args[11];
			}
			if (!args[12].toUpperCase().equals("NULL") && !args[13].toUpperCase().equals("NULL"))
			{
				locKeyWord = args[12];
				locFilter = args[13];
			}
		}

		// 初始化Hadoop系统环境
		Configuration conf = new Configuration();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SuYanPlat))
		{
			String id = MainModel.GetInstance().getAppConfig().getSuYanId();
			String key = MainModel.GetInstance().getAppConfig().getSunYanKey();
			String queue = MainModel.GetInstance().getAppConfig().getSuYanQueue();
			conf.set("hadoop.security.bdoc.access.id", id);
			conf.set("hadoop.security.bdoc.access.key", key);
			conf.set("mapreduce.job.queuename", queue);
		}
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
		{
			String fsurl = "hdfs://" + MainModel.GetInstance().getAppConfig().getHadoopHost() + ":" + MainModel.GetInstance().getAppConfig().getHadoopHdfsPort();
			conf.set("fs.defaultFS", fsurl);
		}
		MainModel.GetInstance().setConf(conf);

		String queueName = args[0];// network
		String statTime = args[1];// 01_151013
		String xdrDataPath_mme = args[2];//
		String xdrDataPath_http = args[3];
		String xdrLocationPath = args[4];

		String statType = args.length >= 6 ? args[5] : "";

		if (xdrDataPath_mme.length() == 0 || xdrDataPath_http.length() == 0)
		{
			System.out.println("xdr data path is null, check the inputs ");
			return;
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

		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		String mroDataPath = MainModel.GetInstance().getAppConfig().getMroDataPath();
		String mtMroDataPath = MainModel.GetInstance().getAppConfig().getMTMroDataPath();
		String mreDataPath = MainModel.GetInstance().getAppConfig().getMreDataPath();
		String mdtLogDataPath = MainModel.GetInstance().getAppConfig().getMdtLogDataPath();
		String mdtImmDataPath = MainModel.GetInstance().getAppConfig().getMdtImmDataPath();
		String mdtRlfDataPath = MainModel.GetInstance().getAppConfig().getMdtRlfDataPath();
		String locWFDataPath = MainModel.GetInstance().getAppConfig().getLocWFDataPath();
		String cellgridpath = MainModel.GetInstance().getAppConfig().getCellgridPath();
		String adjustedSrcPath = MainModel.GetInstance().getAppConfig().getAdjustedSrcPath();
		String figurePath = MainModel.GetInstance().getAppConfig().getSrcFigurePath();
		int size = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSize());
		String eciConfigPath = MainModel.GetInstance().getAppConfig().getEciConfigPath();

		HDFSOper hdfsOper = null;
		if (!mroXdrMergePath.contains(":"))
		{
			hdfsOper = new HDFSOper(conf);
			/*
			 * hdfsOper = new
			 * HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
			 * MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
			 */
		}

		System.out.println("<mroXdrMergePath>: " + mroXdrMergePath);
		System.out.println("<mroDataPath>: " + mroDataPath);
		System.out.println("<mreDataPath>: " + mreDataPath);
		System.out.println("<statType>: " + statType);
		TmpFileFilter.hdfsOper = hdfsOper;

		if (checkDoStat("XDRPREPARE"))
		{
			// 小区统计
			String[] myArgs = new String[8];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrDataPath_mme;
			myArgs[4] = xdrDataPath_http;
			myArgs[5] = String.format("%s/xdr_prepare/data_%s", mroXdrMergePath, statTime);
			myArgs[6] = mmeFilter;
			myArgs[7] = httpFilter;

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append("/user/hive/warehouse/stg.db/o_se_ur_s1mme_tdr/day=20" + statTime.substring(3, 9) + "/hour=0" + i + ",");
					}
					else
					{
						sb.append("/user/hive/warehouse/stg.db/o_se_ur_s1mme_tdr/day=20" + statTime.substring(3, 9) + "/hour=" + i + ",");
					}
				}

				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

				StringBuffer sb1 = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb1.append("/user/hive/warehouse/stg.db/o_se_ur_lte_http_tdr/day=20" + statTime.substring(3, 9) + "/hour=0" + i + ",");
					}
					else
					{
						sb1.append("/user/hive/warehouse/stg.db/o_se_ur_lte_http_tdr/day=20" + statTime.substring(3, 9) + "/hour=" + i + ",");
					}
				}

				myArgs[4] = sb1.toString().substring(0, sb1.toString().length() - 1);

			}

			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append(xdrDataPath_mme + "/0" + i + "/00" + ",");
						sb.append(xdrDataPath_mme + "/0" + i + "/15" + ",");
						sb.append(xdrDataPath_mme + "/0" + i + "/30" + ",");
						sb.append(xdrDataPath_mme + "/0" + i + "/45" + ",");
					}
					else
					{
						sb.append(xdrDataPath_mme + "/" + i + "/00" + ",");
						sb.append(xdrDataPath_mme + "/" + i + "/15" + ",");
						sb.append(xdrDataPath_mme + "/" + i + "/30" + ",");
						sb.append(xdrDataPath_mme + "/" + i + "/45" + ",");
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

				StringBuffer sb1 = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb1.append(xdrDataPath_http + "/0" + i + "/00" + ",");
						sb1.append(xdrDataPath_http + "/0" + i + "/15" + ",");
						sb1.append(xdrDataPath_http + "/0" + i + "/30" + ",");
						sb1.append(xdrDataPath_http + "/0" + i + "/45" + ",");
					}
					else
					{
						sb1.append(xdrDataPath_http + "/" + i + "/00" + ",");
						sb1.append(xdrDataPath_http + "/" + i + "/15" + ",");
						sb1.append(xdrDataPath_http + "/" + i + "/30" + ",");
						sb1.append(xdrDataPath_http + "/" + i + "/45" + ",");
					}
				}
				myArgs[4] = sb1.toString().substring(0, sb1.toString().length() - 1);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.XinJiang))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/MME/20" + statTime.substring(3, 9) + "/0" + i  + ",");
					}
					else
					{
						sb.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/MME/20" + statTime.substring(3, 9) + "/" + i + ",");
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

				StringBuffer sb1 = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb1.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/HTTP/20" + statTime.substring(3, 9) + "/0" + i  + ",");
					}
					else
					{
						sb1.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/HTTP/20" + statTime.substring(3, 9) + "/" + i + ",");
					}
				}
				myArgs[4] = sb1.toString().substring(0, sb1.toString().length() - 1);
			}

			// 增加待处理文件过滤
			TmpFileFilter.mapValidStr.clear();
			if (mmeKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
			}
			if (httpKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
			}
			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job xdrPrepareJob = XdrPrepareMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!xdrPrepareJob.waitForCompletion(true))
				{
					System.out.println("xdr prepare Job error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("XDRPrepare has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("LOCFILL"))
		{
			// 执行loc fill
			String[] myArgs = new String[8];
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrLocationPath;
			// myArgs[4] = String.format("%s/xdr_prepare/data_%s/TB_IMSI_IP_%s",
			// mroXdrMergePath, statTime, statTime);
			myArgs[4] = xdrDataPath_http;
			myArgs[5] = String.format("%s/loc_fill/data_%s", mroXdrMergePath, statTime);
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
			{
				myArgs[5] = String.format("%s/xdr_prepare/data_%s/", mroXdrMergePath, statTime);
			}
			myArgs[6] = mmeFilter;
			myArgs[7] = httpFilter;
			// 增加待处理文件过滤
			TmpFileFilter.mapValidStr.clear();
			if (mmeKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
			}
			if (httpKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
			}

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job mroformatJob = LocImsiFillMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroformatJob.waitForCompletion(true))
				{
					System.out.println("mroformatJob error! stop run.");
					throw (new Exception("system.exit1"));
				}
				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("LOCFILL has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("XDRLOC"))
		{
			String locPath = "";
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
			{
				locPath = String.format("%s/xdr_prepare/data_%s/TB_LOCATION_%s", mroXdrMergePath, statTime, statTime);
			}
			if (locPath.length() > 0)
			{//
				if (!checkDoStat("LOCFILL"))
				{
					locPath = locPath + "," + xdrLocationPath;
				}
			}
			else
			{
				locPath = xdrLocationPath;
			}

			// 增加待处理文件过滤
			TmpFileFilter.mapValidStr.clear();
			if (mmeKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mmeKeyWord, mmeFilter);
			}
			if (httpKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(httpKeyWord, httpFilter);
			}
			if (locKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(locKeyWord, locFilter);
			}
			// 执行xdr运算
			String[] myArgs = new String[13];
			// myArgs[0] = "3000";
			myArgs[0] = "1000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = xdrDataPath_mme;
			myArgs[4] = xdrDataPath_http;
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoXdrHttp))
			{
				myArgs[4] = "NULL";
			}

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append("/user/hive/warehouse/stg.db/o_se_ur_s1mme_tdr/day=20" + statTime.substring(3, 9) + "/hour=0" + i + ",");
					}
					else
					{
						sb.append("/user/hive/warehouse/stg.db/o_se_ur_s1mme_tdr/day=20" + statTime.substring(3, 9) + "/hour=" + i + ",");
					}
				}
				// String mroPath = "/szmt/Data/test/mro";
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

				StringBuffer sb1 = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb1.append("/user/hive/warehouse/stg.db/o_se_ur_lte_http_tdr/day=20" + statTime.substring(3, 9) + "/hour=0" + i + ",");
					}
					else
					{
						sb1.append("/user/hive/warehouse/stg.db/o_se_ur_lte_http_tdr/day=20" + statTime.substring(3, 9) + "/hour=" + i + ",");
					}
				}
				// String mroPath = "/szmt/Data/test/mro";
				myArgs[4] = sb1.toString().substring(0, sb1.toString().length() - 1);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append(xdrDataPath_mme + "/0" + i + "/00" + ",");
						sb.append(xdrDataPath_mme + "/0" + i + "/15" + ",");
						sb.append(xdrDataPath_mme + "/0" + i + "/30" + ",");
						sb.append(xdrDataPath_mme + "/0" + i + "/45" + ",");
					}
					else
					{
						sb.append(xdrDataPath_mme + "/" + i + "/00" + ",");
						sb.append(xdrDataPath_mme + "/" + i + "/15" + ",");
						sb.append(xdrDataPath_mme + "/" + i + "/30" + ",");
						sb.append(xdrDataPath_mme + "/" + i + "/45" + ",");
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

				StringBuffer sb1 = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb1.append(xdrDataPath_http + "/0" + i + "/00" + ",");
						sb1.append(xdrDataPath_http + "/0" + i + "/15" + ",");
						sb1.append(xdrDataPath_http + "/0" + i + "/30" + ",");
						sb1.append(xdrDataPath_http + "/0" + i + "/45" + ",");
					}
					else
					{
						sb1.append(xdrDataPath_http + "/" + i + "/00" + ",");
						sb1.append(xdrDataPath_http + "/" + i + "/15" + ",");
						sb1.append(xdrDataPath_http + "/" + i + "/30" + ",");
						sb1.append(xdrDataPath_http + "/" + i + "/45" + ",");
					}
				}
				myArgs[4] = sb1.toString().substring(0, sb1.toString().length() - 1);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.XinJiang))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/MME/20" + statTime.substring(3, 9) + "/0" + i  + ",");
					}
					else
					{
						sb.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/MME/20" + statTime.substring(3, 9) + "/" + i + ",");
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

				StringBuffer sb1 = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb1.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/HTTP/20" + statTime.substring(3, 9) + "/0" + i  + ",");
					}
					else
					{
						sb1.append("/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/HTTP/20" + statTime.substring(3, 9) + "/" + i + ",");
					}
				}
				myArgs[4] = sb1.toString().substring(0, sb1.toString().length() - 1);
			}

			myArgs[5] = String.format("/flume/23G/location/%s", "20" + statTime.substring(3));// 23g
																								// path
			myArgs[6] = String.format("%s/xdrcellmark", mroXdrMergePath);
			myArgs[7] = locPath;
			myArgs[8] = String.format("%1$s/20%2$s", locWFDataPath, statTime.substring(3, 9));
			myArgs[9] = String.format("%s/xdr_prepare/data_%s/TB_IMSI_COUNT_%s", mroXdrMergePath, statTime, statTime);
			myArgs[10] = String.format("%s/xdr_loc/data_%s", mroXdrMergePath, statTime);

			myArgs[11] = mmeFilter;
			myArgs[12] = httpFilter;
			String _successFile = myArgs[10] + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job xdrLocJob = XdrLableFillMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!xdrLocJob.waitForCompletion(true))
				{
					System.out.println("xdrLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[10], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("XDRLOC has been dealed succesfully:" + myArgs[10]);
			}
		}

		if (checkDoStat("MROFORMATMT"))
		{
			TmpFileFilter.mapValidStr.clear();
			if (mrKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mrKeyWord, mrFilter);
			}
			// 执行mro format
			String[] myArgs = new String[7];
			myArgs[0] = "2000";
			// myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			myArgs[4] = String.format("%1$s/%2$s", mreDataPath, statTime.substring(3, 9));
			myArgs[5] = String.format("%s/mroformat/data_%s", mroXdrMergePath, statTime);

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=ALCATEL,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=DATANG,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=ERICSSON,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=HUAWEI,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=NSN,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=ZTE,");
					}
					else
					{
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=ALCATEL,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=DATANG,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=ERICSSON,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=HUAWEI,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=NSN,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=ZTE,");
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{
				StringBuffer sb = new StringBuffer();
				String minu = "";
				if (!statTime.substring(0, 2).equals("01")) // 测试1小时数据 10点的数据
				{
					for (int i = 0; i < 60; i = i + 15)
					{
						if (i == 0)
						{
							minu = i + "0";
						}
						else
						{
							minu = i + "";
						}
						sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu + "/manu=ERIC,");
						sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu + "/manu=HUAWEI,");
						sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu + "/manu=NOKIA,");
						sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu + "/manu=ZTE,");
					}
				}
				else
				{
					for (int i = 0; i < 24; i++)
					{
						for (int j = 0; j < 60; j = j + 15)
						{
							if (j == 0)
							{
								minu = j + "0";
							}
							else
							{
								minu = j + "";
							}
							if (i < 10)
							{
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m=" + minu + "/manu=ERIC,");
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m=" + minu + "/manu=HUAWEI,");
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m=" + minu + "/manu=NOKIA,");
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m=" + minu + "/manu=ZTE,");
							}
							else
							{
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m=" + minu + "/manu=ERIC,");
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m=" + minu + "/manu=HUAWEI,");
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m=" + minu + "/manu=NOKIA,");
								sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" + statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m=" + minu + "/manu=ZTE,");
							}
						}
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu))
			{
				String mroPath = "/huaweiFITenant/RawDataLayer/Resource/NetResource/O_TYCJ_MRO/20" + statTime.substring(3, 7) + "/20" + statTime.substring(3, 9);
				myArgs[3] = mroPath;
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro))
			{
				String mroPath = "/sichuan/ods/ns/MR/MRO/ERICSSON/20" + statTime.substring(3, 9);
				mroPath += "," + "/sichuan/ods/ns/MR/MRO/HUAWEI/20" + statTime.substring(3, 9);
				mroPath += "," + "/sichuan/ods/ns/MR/MRO/NSN/20" + statTime.substring(3, 9);
				mroPath += "," + "/sichuan/ods/ns/MR/MRO/ZTE/20" + statTime.substring(3, 9);
				myArgs[3] = mroPath;
				myArgs[4] = "NULL";
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append(mtMroDataPath + "/20" + statTime.substring(3, 9) + "/0" + i + ",");
					}
					else
					{
						sb.append(mtMroDataPath + "/20" + statTime.substring(3, 9) + "/" + i + ",");
					}
				}
				myArgs[3] = sb.toString().substring(0, sb.toString().length() - 1);

			}
			myArgs[6] = mrFilter;

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (hdfsOper == null || !hdfsOper.checkFileExist(_successFile))
			{
				Job mroformatJob = MroFormatMTMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroformatJob.waitForCompletion(true))
				{
					System.out.println("mroformatJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROFORMAT has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("MROLOC"))
		{
			TmpFileFilter.mapValidStr.clear();
			if (mrKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mrKeyWord, mrFilter);
			}
			// 执行mro运算
			String[] myArgs = new String[8];
			myArgs[0] = "3000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/xdr_loc/data_%2$s/XDR_LOCATION_%2$s", mroXdrMergePath, statTime);
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro) || MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing)
					|| MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu) || MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng)
					|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi))
			{
				myArgs[4] = String.format("%1$s/mroformat/data_%2$s/mroformat_%2$s", mroXdrMergePath, statTime);
			}
			else
			{
				myArgs[4] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			}
			myArgs[5] = String.format("%s/mro_loc/data_%s", mroXdrMergePath, statTime);
			myArgs[6] = String.format("%1$s/%2$s", mreDataPath, statTime.substring(3, 9));
			myArgs[7] = mrFilter;
			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = MroLableFillMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROLOC has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("MROLOC_NEW"))
		{
			TmpFileFilter.mapValidStr.clear();
			if (mrKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mrKeyWord, mrFilter);
			}
			// 执行mro运算
			String[] myArgs = new String[10];
			myArgs[0] = "10";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/xdr_loc/data_%2$s/XDR_LOCATION_%2$s", mroXdrMergePath, statTime);
			myArgs[4] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			myArgs[5] = String.format("%s/mro_loc/data_%s", mroXdrMergePath, statTime);
			myArgs[6] = String.format("%1$s/%2$s", mreDataPath, statTime.substring(3, 9));
			myArgs[7] = mrFilter;
			myArgs[8] = String.format("%1$s/%2$s", mdtLogDataPath, statTime.substring(3, 9));
			myArgs[9] = String.format("%1$s/%2$s", mdtImmDataPath, statTime.substring(3, 9));

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{
				myArgs[4] = String.format("%1$s/mroformat/data_%2$s/mroformat_%2$s", mroXdrMergePath, statTime);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NoMtMro))
			{
				String mroPath = "/sichuan/ods/ns/MR/MRO/ERICSSON/20" + statTime.substring(3, 9);
				mroPath += "," + "/sichuan/ods/ns/MR/MRO/HUAWEI/20" + statTime.substring(3, 9);
				mroPath += "," + "/sichuan/ods/ns/MR/MRO/NSN/20" + statTime.substring(3, 9);
				mroPath += "," + "/sichuan/ods/ns/MR/MRO/ZTE/20" + statTime.substring(3, 9);
				myArgs[4] = mroPath;
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.LiaoNing))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=ALCATEL,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=DATANG,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=ERICSSON,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=HUAWEI,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=NSN,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=0" + i + "/partitionvendor=ZTE,");
					}
					else
					{
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=ALCATEL,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=DATANG,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=ERICSSON,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=HUAWEI,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=NSN,");
						sb.append("/langchao_mr/lte_mro/province=LN/partitiondate=20" + statTime.substring(3, 9) + "/partitionhour=" + i + "/partitionvendor=ZTE,");
					}
				}
				myArgs[4] = sb.toString().substring(0, sb.toString().length() - 1);
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GanSu))
			{
				String mroPath = "/huaweiFITenant/RawDataLayer/Resource/NetResource/O_TYCJ_MRO/20" + statTime.substring(3, 7) + "/20" + statTime.substring(3, 9);
				myArgs[4] = mroPath;
			}
			else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
			{
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < 24; i++)
				{
					if (i < 10)
					{
						sb.append(mtMroDataPath + "/20" + statTime.substring(3, 9) + "/0" + i + ",");
					}
					else
					{
						sb.append(mtMroDataPath + "/20" + statTime.substring(3, 9) + "/" + i + ",");
					}
				}
				myArgs[4] = sb.toString().substring(0, sb.toString().length() - 1);
			}
			// else if
			// (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			// {
			// StringBuffer sb = new StringBuffer();
			// String minu = "";
			// if (!statTime.substring(0, 2).equals("01")) // 测试1小时数据 10点的数据
			// {
			// for (int i = 0; i < 60; i = i + 15)
			// {
			// if (i == 0)
			// {
			// minu = i + "0";
			// }
			// else
			// {
			// minu = i + "";
			// }
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu
			// + "/manu=ERIC,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu
			// + "/manu=HUAWEI,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu
			// + "/manu=NOKIA,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=10/load_time_m=" + minu
			// + "/manu=ZTE,");
			// }
			// }
			// else
			// {
			// for (int i = 0; i < 24; i++)
			// {
			// for (int j = 0; j < 60; j = j + 15)
			// {
			// if (j == 0)
			// {
			// minu = j + "0";
			// }
			// else
			// {
			// minu = j + "";
			// }
			// if (i < 10)
			// {
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m="
			// + minu + "/manu=ERIC,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m="
			// + minu + "/manu=HUAWEI,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m="
			// + minu + "/manu=NOKIA,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=0" + i + "/load_time_m="
			// + minu + "/manu=ZTE,");
			// }
			// else
			// {
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m="
			// + minu + "/manu=ERIC,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m="
			// + minu + "/manu=HUAWEI,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m="
			// + minu + "/manu=NOKIA,");
			// sb.append("/user/lixiushan/O_MR_MRO_LTE_NCELL/load_time_d=20" +
			// statTime.substring(3, 9) + "/load_time_h=" + i + "/load_time_m="
			// + minu + "/manu=ZTE,");
			// }
			// }
			// }
			// }
			// myArgs[4] = sb.toString().substring(0, sb.toString().length() -
			// 1);
			// }

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (mroXdrMergePath.contains(":") || !hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = MroLableFillMains.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				if (!mroXdrMergePath.contains(":"))
				{
					hdfsOper.mkfile(timeFileName);
				}
			}
			else
			{
				System.out.println("MROLOC_NEW has bend dealed succesfully:" + myArgs[5]);
			}
		}
		if (checkDoStat("MROLOC_UEMR"))
		{
			TmpFileFilter.mapValidStr.clear();
			if (mrKeyWord.length() > 0)
			{
				TmpFileFilter.mapValidStr.put(mrKeyWord, mrFilter);
			}
			// 执行mro运算
			String[] myArgs = new String[7];
			myArgs[0] = "3000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/xdr_loc/data_%2$s/XDR_LOCATION_%2$s", mroXdrMergePath, statTime);

			myArgs[4] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			myArgs[5] = String.format("%s/mro_loc/data_%s", mroXdrMergePath, statTime);
			myArgs[6] = String.format("%1$s/%2$s", mreDataPath, statTime.substring(3, 9));

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = UEMroLableFillExMain.CreateJob(myArgs);
				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROLOC_UEMR has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("CELLGRIDMERGE"))
		{
			String[] myArgs = new String[4];
			myArgs[0] = "1";
			myArgs[1] = queueName;
			myArgs[2] = cellgridpath;
			myArgs[3] = adjustedSrcPath;
			String _successFile = "";
			if (size == 10)
			{
				_successFile = myArgs[3] + "/cellGridMerge/10/output/_SUCCESS";
			}
			else if (size == 40)
			{
				_successFile = myArgs[3] + "/cellGridMerge/40/output/_SUCCESS";
			}
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mergeCellgridJob = MergeCellGridMain.CreateJob(myArgs);
				if (!mergeCellgridJob.waitForCompletion(true))
				{
					System.out.println("mergeCellgrid error! stop run.");
					throw (new Exception("system.exit1"));
				}
			}
			else
			{
				System.out.println("MROLOC has bend dealed succesfully!");
			}
		}
		if (checkDoStat("GRIDECITABLESTAT"))
		{
			String[] myArgs = new String[5];
			String _successFile = "";
			myArgs[0] = "1";
			myArgs[1] = queueName;
			myArgs[3] = figurePath;
			myArgs[4] = adjustedSrcPath;
			if (size == 10)
			{
				myArgs[2] = adjustedSrcPath + "/cellGridMerge/10/MergedCellGridData";
				_successFile = myArgs[4] + "/GridEciTable/10/output/_SUCCESS";
			}
			else if (size == 40)
			{
				myArgs[2] = adjustedSrcPath + "/cellGridMerge/40/MergedCellGridData";
				_successFile = myArgs[4] + "/GridEciTable/40/output/_SUCCESS";
			}
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job GridEciTableJob = CreateGridEciTableMain.CreateJob(myArgs);
				if (!GridEciTableJob.waitForCompletion(true))
				{
					System.out.println("gridEciTable  job error! stop run.");
					throw (new Exception("system.exit1"));
				}
			}
			else
			{
				System.out.println("gridEciTable  has bend dealed succesfully!");
			}
		}
		if (checkDoStat("ECICELLGRIDSTAT"))
		{
			String[] myArgs = new String[5];
			String _successFile = "";
			myArgs[0] = "1";
			myArgs[1] = queueName;
			myArgs[4] = adjustedSrcPath;
			if (size == 10)
			{
				myArgs[2] = adjustedSrcPath + "/GridEciTable/10/GridEciData";
				myArgs[3] = adjustedSrcPath + "/cellGridMerge/10/MergedCellGridData";
				_successFile = myArgs[4] + "/EciCellGridTable/10/output/_SUCCESS";
			}
			else if (size == 40)
			{
				myArgs[2] = adjustedSrcPath + "/GridEciTable/40/GridEciData";
				myArgs[3] = adjustedSrcPath + "/cellGridMerge/40/MergedCellGridData";
				_successFile = myArgs[4] + "/EciCellGridTable/40/output/_SUCCESS";
			}
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job CreateEciCellGridTableJob = CreateEciCellGridTableMain.CreateJob(myArgs);
				if (!CreateEciCellGridTableJob.waitForCompletion(true))
				{
					System.out.println("CreateEciCellGridTable  job error! stop run.");
					throw (new Exception("system.exit1"));
				}
			}
			else
			{
				System.out.println("CreateEciCellGridTableJob  has bend dealed succesfully!");
			}
		}
		if (checkDoStat("ADJUSTFG"))
		{
			String[] myArgs = new String[4];
			String _successFile = "";
			myArgs[0] = "100";
			myArgs[1] = queueName;
			myArgs[2] = eciConfigPath;
			myArgs[3] = adjustedSrcPath;
			if (size == 10)
			{
				_successFile = myArgs[3] + "/adjusted/10/output/_SUCCESS";
			}
			else if (size == 40)
			{
				_successFile = myArgs[3] + "/adjusted/40/output/_SUCCESS";
			}
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job AdjustFigureJob = AdjustFigureMain.CreateJob(myArgs);
				if (!AdjustFigureJob.waitForCompletion(true))
				{
					System.out.println("AdjustFigureJob error! stop run.");
					throw (new Exception("system.exit1"));
				}
			}
			else
			{
				System.out.println("AdjustFigureJob  has bend dealed succesfully!");
			}
		}

		if (checkDoStat("MROLOCFG"))
		{
			// 执行mro运算
			String[] myArgs = new String[8];
			myArgs[0] = "3000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/xdr_loc/data_%2$s/XDR_LOCATION_%2$s", mroXdrMergePath, statTime);

			myArgs[4] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));// mropath
			myArgs[5] = "NULL";
			myArgs[6] = String.format("%s/figure_data", mroXdrMergePath);// figureku
			myArgs[7] = String.format("%s/mro_loc/data_%s", mroXdrMergePath, statTime);

			String _successFile = myArgs[7] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = MroFgLableFillMains.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[7], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROLOC has bend dealed succesfully:" + myArgs[7]);
			}
		}

		if (checkDoStat("MROLOCUE") && statType.length() > 0)
		{
			// 执行mro运算
			String[] myArgs = new String[6];
			myArgs[0] = "3000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("%1$s/xdr_prepare/data_%2$s/TB_LOCATION_%2$s", mroXdrMergePath, statTime);
			myArgs[4] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			myArgs[5] = String.format("%s/mro_loc/data_%s", mroXdrMergePath, statTime);

			String _successFile = myArgs[5] + "/output/_SUCCESS";
			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = UEMroLableFillMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROLOC has bend dealed succesfully:" + myArgs[5]);
			}
		}

		if (checkDoStat("MROVILLAGESTAT"))
		{
			// 执行mro village运算
			String[] myArgs = new String[7];
			myArgs[0] = "1000";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			myArgs[3] = String.format("/mt_wlyh/Data/config/village_grid", statTime);
			// myArgs[4] =
			// String.format("%1$s/mroformat/data_%2$s/mroformat_%2$s",
			// mroXdrMergePath, statTime);
			myArgs[4] = String.format("%1$s/%2$s", mtMroDataPath, statTime.substring(3, 9));
			myArgs[5] = String.format("%s/mro_village/data_%s", mroXdrMergePath, statTime);
			myArgs[6] = String.format("%1$s/%2$s", mreDataPath, statTime.substring(3, 9));
			String _successFile = myArgs[5] + "/output/_SUCCESS";

			if (!hdfsOper.checkFileExist(_successFile))
			{
				Job mroLocJob = VillageStatMain.CreateJob(myArgs);

				Date stime = new Date();

				if (!mroLocJob.waitForCompletion(true))
				{
					System.out.println("mroLocJob error! stop run.");
					throw (new Exception("system.exit1"));
				}

				Date etime = new Date();
				int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
				String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[5], mins, timeFormat.format(stime), timeFormat.format(etime));
				hdfsOper.mkfile(timeFileName);
			}
			else
			{
				System.out.println("MROVILLAGESTAT has bend dealed succesfully:" + myArgs[5]);
			}
		}

		///////////////////////////////////////////////// 2,3G
		///////////////////////////////////////////////// //////////////////////////////////////////////////////
		if (checkDoStat("XDRLOC23G"))
		{
			// 执行xdr运算
			String[] myArgs = new String[5];
			myArgs[0] = "10";
			myArgs[1] = queueName;
			myArgs[2] = statTime;
			// myArgs[3] = String.format("/23Glocation_20160113");//23g input
			// path
			myArgs[3] = String.format("/flume/chun23glocation/%s", "20" + statTime.substring(3));// 23g
			myArgs[4] = String.format("%s/xdr_loc_23g/data_%s", mroXdrMergePath, statTime);
			Job xdrLocJob = XdrLableFill23GMain.CreateJob(myArgs);

			Date stime = new Date();

			if (!xdrLocJob.waitForCompletion(true))
			{
				System.out.println("xdrLoc23gJob error! stop run.");
				throw (new Exception("system.exit1"));
			}

			Date etime = new Date();
			int mins = (int) (etime.getTime() / 1000L - stime.getTime() / 1000L) / 60;
			String timeFileName = String.format("%s/%dMins_%s_%s", myArgs[4], mins, timeFormat.format(stime), timeFormat.format(etime));
			hdfsOper.mkfile(timeFileName);
		}

		///////////////////////////////////////////////// MERGE STAT
		///////////////////////////////////////////////// //////////////////////////////////////////////////////
		if (checkDoStat("MERGESTAT") || checkDoStat("MERGESTAT1"))
		{
//			Mergestat_Tb_Signal.doMergestat1(queueName, statTime, mroXdrMergePath, conf, hdfsOper);
		}
		if (checkDoStat("MERGESTAT2"))
		{
//			Mergestat_Tb_Fg.doMergestat2(queueName, statTime, mroXdrMergePath, conf, hdfsOper);
		}
		if (checkDoStat("MERGESTAT3"))
		{
//			Mergestat_Tb_Mr.doMergestat3(queueName, statTime, mroXdrMergePath, conf, hdfsOper);
		}
		if (checkDoStat("MERGESTAT4"))
		{
//			Mergestat_Round.doMergestat4(queueName, statTime, mroXdrMergePath, conf, hdfsOper);
		}

		if (checkDoStat("XDRLOCALL"))
		{
			LocAllEXMain.localMain(args);
		}

		if (checkDoStat("MERGESTATALL"))
		{
			MergestatGroup.doMergestatGroup(queueName, statTime, mroXdrMergePath, conf, hdfsOper);
		}
	}

	private static boolean checkDoStat(String type)
	{
		if (statTypeMap.values().size() == 0)
		{
			return true;
		}
		return statTypeMap.containsKey(type) ? true : false;
	}

	public static boolean hwRegist()
	{
		// Init environment, load xml file
		Configuration conf = SecurityUtils.getConfiguration();

		// Security login
		if (!SecurityUtils.login(conf))
		{
			return false;
		}

		return true;
	}

}
