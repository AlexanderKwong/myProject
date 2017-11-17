package mro.lablefill_mro;

import java.io.IOException;
import java.util.List;

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
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mro.lablefill.StatDeal;
import mro.lablefill.StatDeal_CQT;
import mro.lablefill.StatDeal_DT;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStatMng;
import mro.lablefill.UserActStat.UserActTime;
import mro.lablefill.UserActStat.UserCell;
import mro.lablefill.UserActStat.UserCellAll;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.MrLocation;
import util.ValidDataGeter;
import xdr.lablefill.ResultHelper;

public class MrDealXdrLocation implements IMrDeal
{
	private Configuration conf;
	private Context context;
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
	
	private StatDeal statDeal;
	private StatDeal_DT statDeal_DT;
	private StatDeal_CQT statDeal_CQT;
	
	private UserActStatMng userActStatMng;
	
	private StringBuilder tmSb = new StringBuilder();
	
	public MrDealXdrLocation(Context context) throws Exception
	{
		conf = context.getConfiguration();
		this.context = context;
		MainModel.GetInstance().setConf(conf);
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
		
		
		mosMng = new MultiOutputMng<NullWritable, Text>(context , MainModel.GetInstance().getFsUrl());
		
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
		
		statDeal = new StatDeal(mosMng);
		statDeal_DT = new StatDeal_DT(mosMng);
		statDeal_CQT = new StatDeal_CQT(mosMng);
		userActStatMng = new UserActStatMng();
	}

	@Override
	public int init(Object[] args)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int deal(Object[] args)
	{
		List<SIGNAL_MR_All> mroItemList = (List<SIGNAL_MR_All>)args[0];
		
		dealSample(mroItemList);

		// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
        statDeal.outDealingData();
        statDeal_DT.outDealingData();
        statDeal_CQT.outDealingData();
        
		// 如果用户数据大于10000个，就吐出去先
		if (userActStatMng.getUserActStatMap().size() > 10000)
		{
			userActStatMng.finalStat();
			
			//用户行动信息输出
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
							
							//主服小区
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
							
							//邻区		
							List<UserCell> userCellList = userActAll.getUserCellList();
							int sn = 1;
                            for(UserCell userCell : userCellList)
                            {
								if(userCell.eci == userActAll.eci)
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
		return 0;
	}

	@Override
	public int finalDeal(Object[] args)
	{
		try
		{
			mosMng.close();
			return 0;
		}
		catch (Exception e)
		{
			// TODO: handle exception
		}
        return 0;
	}

	@Override
	public int output(Object[] args)
	{
        statDeal.outAllData();
        statDeal_DT.outAllData();
        statDeal_CQT.outAllData();
                
		userActStatMng.finalStat();	
		//用户行动信息输出
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
						
						//主服小区
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
						
						//邻区		
						List<UserCell> userCellList = userActAll.getUserCellList();
						int sn = 1;
                        for(UserCell userCell : userCellList)
                        {
							if(userCell.eci == userActAll.eci)
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
		
		return 0;
	}
	
	
	private void dealSample(List<SIGNAL_MR_All> mroList)
	{
		DT_Sample_4G sample = new DT_Sample_4G();
		int dist;
		int maxRadius = 6000;
		
		for (SIGNAL_MR_All data : mroList)
		{
			sample.Clear();
			
			//如果采样点过远就需要筛除
			LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(data.tsc.Eci);
			dist = -1;
			if(lteCellInfo != null)
			{
				if(data.tsc.longitude > 0 && data.tsc.latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
				{
					dist = (int)GisFunction.GetDistance(data.tsc.longitude, data.tsc.latitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);	
				}	
			}	
			data.dist = dist;		
			if(dist < 0 || dist > maxRadius)
			{
				continue;
			}
			
			//基于Ta进行筛
			if(data.tsc.LteScTadv >= 15 && data.tsc.LteScTadv < 1282)
			{
				double taDist = MrLocation.calcDist(data.tsc.LteScTadv , data.tsc.LteScRTTD);
				if(dist > taDist*1.2)
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
		//cpe不参与kpi运算
		if(sample.testType == StaticConfig.TestType_CPE)
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
		tsam.iLAC = (int) ValidDataGeter.getValidData(tsam.iLAC, tTemp.tsc.TAC);
		tsam.iCI = (long) ValidDataGeter.getValidData(tsam.iCI, tTemp.tsc.CellId);
		tsam.Eci = (long) ValidDataGeter.getValidData(tsam.Eci, tTemp.tsc.Eci);
		tsam.eventType = 0;
		tsam.ENBId = (int) ValidDataGeter.getValidData(tsam.ENBId, tTemp.tsc.ENBId);
		tsam.UserLabel = tTemp.tsc.UserLabel;
		tsam.CellId = (long) ValidDataGeter.getValidData(tsam.CellId, tTemp.tsc.CellId);
		tsam.Earfcn = tTemp.tsc.Earfcn;
		tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
		tsam.MmeCode = (int) ValidDataGeter.getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
		tsam.MmeGroupId = (int) ValidDataGeter.getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
		tsam.MmeUeS1apId = (long) ValidDataGeter.getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
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
				
		if(tTemp.tsc.EventType.length() > 0)
		{
			if(tTemp.tsc.EventType.equals("MRO"))
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

			if (tsam.testType == StaticConfig.TestType_DT)
			{
				if(tsam.ilongitude > 0)
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("sampledt", NullWritable.get(), curText);
				}
			}
			else if (tsam.testType == StaticConfig.TestType_DT_EX
				   || tsam.testType == StaticConfig.TestType_CPE)
			{
				if(tsam.ilongitude > 0)
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

			if(MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
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
		if((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150)
		    && tsam.LteScRSRP > -110)
		{
			for(NC_LTE item : tsam.tlte)
			{
				if((item.LteNcRSRP < -50 && item.LteNcRSRP > -150)
				  && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if(tsam.Earfcn == item.LteNcEarfcn)
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
