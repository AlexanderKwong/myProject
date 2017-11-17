package imsiResident.buildloc;

import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import mroxdrmerge.MainModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cellconfig.CellBuildInfo;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;

public class ImsiResidentReducer
{
	public static class ImsiResidentReduce extends DataDealReducer<ImsiResidentKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private HashMap<String, Integer> imsiGridMap;
		private CellBuildInfo cellBuild;
		private Text curText = new Text();
		private String mergedOutPutPath;
		private Context context;
		private long tempEci;// 记录上一个eci

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			mergedOutPutPath = conf.get("mastercom.mroxdrmerge.mergedOutPutPath");
			this.context = context;
			if (!mergedOutPutPath.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			mosMng.SetOutputPath("ImsiResident", mergedOutPutPath);
			mosMng.init();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
			mosMng.close();
		}

		@Override
		protected void reduce(ImsiResidentKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			imsiGridMap = new HashMap<String, Integer>();
			ArrayList<String> list = new ArrayList<String>();
			String vals[];
			String keys = "";
			String str = "";
			long longtitude = 0;
			long latitude = 0;
			int averTimes = 0;
			int totalTime = 0;
			int timeMax = 0;
			String longtitude_latitude = "";
			String buildInfo = "";

			if (key.getEci() != tempEci)
			{
				// 初始化小区楼宇表
				LteCellInfo cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
				cellInfo = CellConfig.GetInstance().getLteCell(key.getEci());
				if (cellInfo == null)// cell统计需要全量，不能抛弃
				{
					cellInfo = new LteCellInfo();
					LOGHelper.GetLogger().writeLog(LogType.info,
							"gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:" + key.getEci() + "  enbid:" + key.getEci() / 256 + " cellid:" + key.getEci() % 256);
				}
				if (!cellBuild.loadCellBuild(conf, (int) key.getEci(), cellInfo.cityid))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "cellbuild init error 请检查！eci:" + key.getEci() + " map.size:" + cellBuild.getCellBuildMap().size());
				}
				tempEci = key.getEci();
			}

			for (Text temp : values)
			{
				list.add(temp.toString());
				vals = temp.toString().split(",", -1);
				averTimes = Integer.parseInt(vals[3]);
				if (Long.parseLong(vals[4]) <= 0)
				{
					continue;
				}
				longtitude = Long.parseLong(vals[4]) / 1000 * 1000;
				latitude = Long.parseLong(vals[5]) / 900 * 900 + 900;
				keys = longtitude + "_" + latitude;

				if (imsiGridMap.containsKey(keys))
				{
					totalTime = imsiGridMap.get(keys);
					totalTime += averTimes;
					imsiGridMap.put(keys, totalTime);
					if (totalTime > timeMax)
					{
						timeMax = totalTime;
						longtitude_latitude = keys;
					}
				}
				else
				{
					imsiGridMap.put(keys, averTimes);
					if (averTimes > timeMax)
					{
						timeMax = averTimes;
						longtitude_latitude = keys;
					}
				}
			}

			if (cellBuild.getCellBuildMap().containsKey(longtitude_latitude))
			{
				buildInfo = cellBuild.getCellBuildMap().get(longtitude_latitude) + "," + longtitude_latitude.replace("_", ",");
			}
			else
			{
				buildInfo = "0,0,0";
			}
			for (String temp : list)
			{
				str = temp + "," + buildInfo;
				curText.set(str);
				mosMng.write("ImsiResident", NullWritable.get(), curText);
			}
		}
	}
}
