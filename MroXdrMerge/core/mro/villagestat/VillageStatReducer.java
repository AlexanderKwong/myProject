package mro.villagestat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.DT_Sample_4G;
import StructData.GridItem;
import StructData.NC_LTE;
import StructData.SIGNAL_MR_All;
import StructData.Stat_Grid_4G;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import jan.util.TimeHelper;
import mroxdrmerge.MainModel;
import util.LteStatHelper;
import xdr.lablefill.ResultHelper;

public class VillageStatReducer
{

	public static class MroDataFileReducer extends DataDealReducer<GridTypeKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();

		private String path_sample;
		private String path_grid;

		private String[] strs;
		
		private int villageLongitude;
		private int villageLatitude;
		private int cityid;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			Configuration conf = context.getConfiguration();
			path_sample = conf.get("mastercom.mroxdrmerge.mro.villagestat.path_sample");
			path_grid = conf.get("mastercom.mroxdrmerge.mro.villagestat.path_grid");

			this.context = context;

			// 初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.SetOutputPath("mrosample", path_sample);
			mosMng.SetOutputPath("mrogrid", path_grid);

			mosMng.init();

			////////////////////
			villageLongitude = 0;
			villageLatitude = 0;
			cityid = 0;

		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);

			mosMng.close();
		}

		@Override
		public void reduce(GridTypeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			
			if (key.getDataType() == 1)
			{
				villageLongitude = key.getTllong();
				villageLatitude = key.getTllat();	
			}
			else if(key.getDataType() == 100)
			{
				if(key.getTllong() != villageLongitude
				|| key.getTllat() != villageLatitude)
				{
					return;
				}			

				List<SIGNAL_MR_All> mroItemList = new ArrayList<SIGNAL_MR_All>();
				Text value;
				int longitude;
				int latitude;
				while (values.iterator().hasNext())
				{
					value = values.iterator().next();
					
					strs = value.toString().split(StaticConfig.DataSliper2, -1);
					
					cityid = Integer.parseInt(strs[0]);
					longitude = Integer.parseInt(strs[1]);
					latitude = Integer.parseInt(strs[2]);

					SIGNAL_MR_All mroItem = new SIGNAL_MR_All();
					Integer i = 3;
					try
					{
						mroItem.FillData(new Object[] { strs, i });
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "SIGNAL_MR_All.FillData error ", e);
						continue;
					}

					if (mroItem.tsc.MmeUeS1apId <= 0 || mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "mro format err :  " + value);
						continue;
					}
					
					mroItem.tsc.cityID = cityid;
					mroItem.tsc.longitude = longitude;
					mroItem.tsc.latitude = latitude;

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
		}


		private void dealSample(List<SIGNAL_MR_All> mroList)
		{
			if(mroList.size() <= 0)
			{
				return;
			}

			DT_Sample_4G sample = new DT_Sample_4G();
			Map<Integer, Stat_Grid_4G> lteGridMap = new HashMap<Integer, Stat_Grid_4G>();
			for (SIGNAL_MR_All data : mroList)
			{
				sample.Clear();

				statMro(sample, data);		
				statKpi(sample, lteGridMap);
			}
			
			try
			{
				for(Stat_Grid_4G item : lteGridMap.values())
				{
					curText.set(ResultHelper.getPutGrid_4G(item));
					mosMng.write("mrogrid", NullWritable.get(), curText);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "write error : ", e);
			}
		}

		private void statKpi(DT_Sample_4G sample, Map<Integer, Stat_Grid_4G> lteGridMap)
		{
			Stat_Grid_4G lteGrid = lteGridMap.get(sample.cityID);
			if(lteGrid == null)
			{
			    lteGrid = new Stat_Grid_4G();
				GridItem gridItem = GridItem.GetGridItem(sample.cityID, sample.ilongitude, sample.ilatitude);
				lteGrid.startTime = TimeHelper.getRoundDayTime(sample.itime);
				lteGrid.endTime = lteGrid.startTime + 86400;
				lteGrid.icityid = sample.cityID;
				lteGrid.itllongitude = gridItem.getTLLongitude();
				lteGrid.itllatitude = gridItem.getTLLatitude();
				lteGrid.ibrlongitude = gridItem.getBRLongitude();
				lteGrid.ibrlatitude = gridItem.getBRLatitude();
				
				lteGridMap.put(sample.cityID, lteGrid);
			}
			
			lteGrid.isamplenum++;
			lteGrid.MrCount++;		
			LteStatHelper.statMro(sample, lteGrid.tStat);
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

			try
			{
				//cai test 
				 //curText.set(ResultHelper.getPutLteVillageSample(tsam));
				 curText.set(ResultHelper.getPutLteSample(tsam));
				 mosMng.write("mrosample", NullWritable.get(), curText);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "output event error ", e);
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
