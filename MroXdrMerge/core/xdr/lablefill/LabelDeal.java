package xdr.lablefill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.SIGNAL_LOC;
import StructData.SIGNAL_XDR_4G;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import imsiCellTime.Merge.ImeiCellTimesKey;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.GisFunction;
import mdtstat.Util;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class LabelDeal
{
	private long imsi;
	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText = new Text();

	private int totalDistance;
	private int totalDuration;

	private SIGNAL_LOC lastOldXdrItem;
	private SIGNAL_LOC estiLastOldXdrItem;

	private boolean isDDDriver;

	private Map<String, TimeSpan> userHomeCellMap = new HashMap<String, TimeSpan>();
	private String curCell;
	private int curSTime;
	private int lastTime;

	public LabelDeal(long imsi, MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.imsi = imsi;
		this.mosMng = mosMng;

		totalDistance = 0;
		totalDuration = 0;

		lastOldXdrItem = null;

		isDDDriver = false;

		curCell = "";
		curSTime = 0;
		lastTime = 0;
	}

	public long getImsi()
	{
		return imsi;
	}

	public boolean IsDDDriver()
	{
		return isDDDriver;
	}

	public void setDDDriver(boolean isDDDriver)
	{
		this.isDDDriver = isDDDriver;
	}

	public Map<String, TimeSpan> getUserHomeCellMap()
	{
		return userHomeCellMap;
	}

	public boolean deal(List<? extends SIGNAL_LOC> xdrItemList)
	{
		if (xdrItemList.size() == 0)
		{
			return false;
		}

		int totalDistance_10Min = 0;
		int totalDuration_10Min = 0;
		boolean flag = false;// 标记这一批数据是否全部没有经纬度

		// 排序后，对用户的TestTypeGL进行回填，用于后续location关联
		Collections.sort(xdrItemList);

		List<SIGNAL_LOC> labelList = new ArrayList<SIGNAL_LOC>();
		int oldLongitude = 0;
		int oldLatitude = 0;
		int oldTime = 0;
		Map<String, Integer> cellMap = new HashMap<String, Integer>();

		if (lastOldXdrItem != null)
		{
			// 如果上一次遗留的点离当前点太久了，就已经不具有参考意义了
			if (xdrItemList.get(0).stime / 600 * 600 - lastOldXdrItem.stime / 600 * 600 >= 1200)
			{
				lastOldXdrItem = null;
			}
		}

		// 添加上次遗留最后的一个点
		if (lastOldXdrItem != null)
		{
			oldLongitude = lastOldXdrItem.longitude;
			oldLatitude = lastOldXdrItem.latitude;
			oldTime = lastOldXdrItem.stime;
			labelList.add(lastOldXdrItem);
		}

		// long curEci = xdrItemList.get(0).Eci;
		// int curSTime = xdrItemList.get(0).stime;

		for (int i = 0; i < xdrItemList.size(); i++)
		{
			SIGNAL_LOC xdrItem = xdrItemList.get(i);
			lastTime = xdrItem.stime;
			// 判断：如果用户存在 location=3，那么遇到Dt测试就加入dt数据
			if (xdrItem.location == 3)
			{
				isDDDriver = true;
			}
			if (!cellMap.containsKey(xdrItem.GetCellKey()))
			{
				cellMap.put(xdrItem.GetCellKey(), 0);
			}
			formatData(xdrItem);

			// 计算用户长时间驻留小区
			if (!curCell.equals(xdrItem.GetCellKey()))
			{
				if (curSTime > 0 && lastTime - curSTime >= 3600 * 2)
				{
					TimeSpan timeSpan = userHomeCellMap.get(curCell);
					if (timeSpan == null)
					{
						timeSpan = new TimeSpan();
						timeSpan.stime = curSTime;
						timeSpan.etime = lastTime;
						userHomeCellMap.put(curCell, timeSpan);
					}
					else
					{
						if (timeSpan.getDuration() < lastTime - curSTime)
						{
							timeSpan.stime = curSTime;
							timeSpan.etime = lastTime;
						}
					}
				}
				curCell = xdrItem.GetCellKey();
				curSTime = lastTime;
			}

			if (xdrItem.longitude <= 0)
			{
				continue;
			}
			if (xdrItem.radius > 100)
			{
				continue;
			}

			flag = true;

			if (oldLongitude > 0)
			{
				double curDis = GisFunction.GetDistance(oldLongitude, oldLatitude, xdrItem.longitude, xdrItem.latitude);
				int curDur = xdrItem.stime - oldTime;

				double curSpeed = curDur > 0 ? (curDis * 3600.0) / (curDur * 1000.0) : 0;
				int curAngle = GisFunction.GetAngleFromPointToPoint(oldLongitude, oldLatitude, xdrItem.longitude, xdrItem.latitude);

				// 判别跳变的采样点，将速度置为零
				if (curSpeed >= 400/* || curDis < 200 */)
				{
					curSpeed = 0;
					curAngle = -1;
					curDis = 0;
					curDur = 0;
				}

				xdrItem.mt_speed = curSpeed;
				xdrItem.mt_label = "unknow";
				xdrItem.moveDirect = curAngle;

				///////////////////////////////////
				totalDistance_10Min += curDis;
				totalDuration_10Min += curDur;

				totalDistance += curDis;
				totalDuration += curDur;
			}

			oldLongitude = xdrItem.longitude;
			oldLatitude = xdrItem.latitude;
			oldTime = xdrItem.stime;

			labelList.add(xdrItem);
		}

		if (labelList.size() > 1)
		{
			// 这是计算label
			if (cellMap.size() <= 2 && checkStatic(labelList))
			{
				for (SIGNAL_LOC item : labelList)
				{
					item.mt_label = "static";

					// 精度大于80的，不会参与回填，经纬度已经失真
					if (item.radius > 80)
					{
						String[] strs = labelList.get(0).valStr.split("\t", -1);
						if (strs.length == 47)// 输出4g
						{
							curText.set(labelList.get(0).valStr + "\t" + "处在静止状态但精度不够，忽略");

							try
							{
								if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
									mosMng.write("eventerr", NullWritable.get(), curText);
							}
							catch (Exception e)
							{
								// TODO: handle exception
							}
						}
					}
				}
			}
			else
			{
				// 如果用户10分钟内的均速为15km/h以上，则是高速点。
				// 假如10分钟均速小于20Km/h，那就参考瞬间速度大于20Km/h为高速
				double speed_10Min = totalDuration_10Min > 0 ? (totalDistance_10Min * 3600.0) / (totalDuration_10Min * 1000.0) : 0;
				if (speed_10Min >= 15)
				{
					for (SIGNAL_LOC item : labelList)
					{
						item.mt_label = "high";
					}
				}
				else
				{
					for (int i = 1; i < labelList.size(); ++i)
					{
						SIGNAL_LOC item = labelList.get(i);

						if (item.mt_speed >= 20)
						{
							item.mt_label = "high";

							SIGNAL_LOC preItem = labelList.get(i - 1);
							if (preItem.mt_label.equals("unknow"))
							{
								preItem.mt_label = "high";
								preItem.mt_speed = item.mt_speed;
							}
						}
						else
						{
							item.mt_label = "low";

							SIGNAL_LOC preItem = labelList.get(i - 1);
							if (preItem.mt_label.equals("unknow"))
							{
								preItem.mt_label = "low";
								preItem.mt_speed = item.mt_speed;
							}
						}
					}
				}
			}

		}

		// 孤立点无法判断运动状态，当作是低速点
		if (labelList.size() == 1 && lastOldXdrItem == null)
		{
			SIGNAL_LOC preItem = labelList.get(0);
			preItem.mt_label = "low";
			preItem.mt_speed = 0;
		}

		if (labelList.size() > 0)
		{
			lastOldXdrItem = labelList.get(labelList.size() - 1);
		}
		return flag;
	}

	/**
	 * 
	 * @param xdrItemList
	 *            10分钟同一用户的xdr数据
	 * @param locationImsiCellLocMng
	 *            同一用户全天的常驻小区列表
	 */
	public void estiLabelDeal(List<? extends SIGNAL_LOC> xdrItemList, LocationImsiCellLocMng locationImsiCellLocMng)
	{
		String type = "";// 用来标识10min用户的运动状态
		if (xdrItemList.size() == 0)
		{
			return;
		}
		double totalDistance = 0;// 三十分钟的运动距离
		int totalDuration = 0;// 运动这段距离所用的时间
		List<SIGNAL_LOC> labelList = new ArrayList<SIGNAL_LOC>();
		int oldLongitude = 0;
		int oldLatitude = 0;
		int oldTime = 0;

		int curLongtitude = 0;
		int curLatitude = 0;
		int curTime = 0;
		double avg_speed = 0;
		Map<String, Integer> cellMap = new HashMap<String, Integer>();
		if (estiLastOldXdrItem != null)
		{
			// 如果上一次遗留的点离当前点太久了，就已经不具有参考意义了
			if (xdrItemList.get(0).stime / 1800 * 1800 - estiLastOldXdrItem.stime / 1800 * 1800 >= 3600)
			{
				estiLastOldXdrItem = null;
			}
		}
		// 添加上次遗留最后的一个点
		if (estiLastOldXdrItem != null)
		{
			oldLongitude = estiLastOldXdrItem.longitude;
			oldLatitude = estiLastOldXdrItem.latitude;
			oldTime = estiLastOldXdrItem.stime;
			labelList.add(estiLastOldXdrItem);
		}
		// 判断该用户的运动状态
		for (SIGNAL_LOC temp : xdrItemList)
		{
			SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) temp;
			if (!cellMap.containsKey(xdrItem.GetCellKey()))
			{
				cellMap.put(xdrItem.GetCellKey(), 0);
			}
			LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(xdrItem.Eci);
			if (lteCellInfo != null)
			{
				curLongtitude = lteCellInfo.ilongitude;
				curLatitude = lteCellInfo.ilatitude;
				curTime = xdrItem.stime;
				if (oldLongitude > 0)
				{
					double curDis = GisFunction.GetDistance(oldLongitude, oldLatitude, curLongtitude, curLatitude);
					int curDur = curTime - oldTime;
					totalDistance = totalDistance + curDis;
					totalDuration = totalDuration + curDur;
				}
				oldLongitude = lteCellInfo.ilongitude;
				oldLatitude = lteCellInfo.ilatitude;
				oldTime = xdrItem.stime;

				labelList.add(xdrItem);
			}
		}

		avg_speed = (totalDuration > 0 ? (totalDistance * 3600.0) / (totalDuration * 1000.0) : 0);
		if (labelList.size() > 1)
		{
			if (cellMap.size() <= 2 && avg_speed <= 3)// 按照十分钟内两小区切换距离500m计算
			{
				type = "esti_static";
			}
		}
		if (labelList.size() > 0)
		{
			estiLastOldXdrItem = labelList.get(labelList.size() - 1);
		}

		// 利用常驻小区经纬度回填
		if (type.equals("esti_static"))
		{
			for (SIGNAL_LOC temp : xdrItemList)
			{
				SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) temp;
				int dayHour = Util.getHour(xdrItem.stime);
				ImeiCellTimesKey tempkey = new ImeiCellTimesKey(xdrItem.IMSI, dayHour, xdrItem.Eci);
				LocationImsiCellTime imsicell = locationImsiCellLocMng.getItem(tempkey);
				if (xdrItem.longitude <= 0 && imsicell != null)
				{
					xdrItem.longitude = (int) imsicell.longtitude;
					xdrItem.latitude = (int) imsicell.latitude;
					xdrItem.mt_label = type;
					xdrItem.location = 10;
					xdrItem.loctp = "fp";
				}
			}
		}
	}

	// 对数据进行格式化，标准化
	public void formatData(SIGNAL_LOC xdrItem)
	{
		// 嘀嘀司机loctp进行转换
		if (xdrItem.location == 2 && xdrItem.longitude > 0)
		{
			if (xdrItem.loctp.equals(""))
			{
				xdrItem.loctp = "ll";
				xdrItem.radius = 0;
			}
		}

		// 高德地图loctp进行转换
		/*
		 * if(xdrItem.location == 5) { if(xdrItem.loctp.equals("3")) {
		 * xdrItem.loctp = "cl"; } else if(xdrItem.loctp.equals("14") ||
		 * xdrItem.loctp.equals("24") || xdrItem.loctp.equals("5") ||
		 * xdrItem.loctp.equals("wf")) { xdrItem.loctp = "wf"; } else {
		 * xdrItem.loctp = "unknow"; } }
		 */

		// 腾讯地图loctp需要转换
		if (xdrItem.location == 6)
		{
			if (xdrItem.radius <= 20 && xdrItem.radius >= 0)
			{
				xdrItem.loctp = "ll";
			}
			else if (xdrItem.radius >= 80)
			{
				// 腾讯定位精度>80的，都没有参考意义
				xdrItem.longitude = 0;
				xdrItem.latitude = 0;
				xdrItem.loctp = "unknow";
			}
		}

		// 如果location 是 0，1 那么将经纬度抹掉
		if (xdrItem.location >= 0 && xdrItem.location <= 1)
		{
			xdrItem.longitude = 0;
			xdrItem.latitude = 0;
		}

		if (xdrItem.lable.length() == 0)
		{
			xdrItem.lable = "unknow";
		}

		if (xdrItem.location == 4 && xdrItem.loctp.length() == 0)
		{
			// 如果是unknow，说明该sdk的定位体系并不明确，不能用
			xdrItem.loctp = "unknow";
			xdrItem.longitude = 0;
			xdrItem.latitude = 0;
		}
	}

	public void finalDeal()
	{
		if (curSTime > 0 && lastTime - curSTime >= 3600 * 2)
		{
			TimeSpan timeSpan = userHomeCellMap.get(curCell);
			if (timeSpan == null)
			{
				timeSpan = new TimeSpan();
				timeSpan.stime = curSTime;
				timeSpan.etime = lastTime;
				userHomeCellMap.put(curCell, timeSpan);
			}
			else
			{
				if (timeSpan.getDuration() < lastTime - curSTime)
				{
					timeSpan.stime = curSTime;
					timeSpan.etime = lastTime;
				}
			}
		}

	}

	// 如果用户10分钟内多个经纬度点都是在100米内，就说明用户当前处在静止的状态
	public boolean checkStatic(List<SIGNAL_LOC> labelList)
	{
		if (labelList.size() <= 1)
		{
			return false;
		}

		int tllongitude = Integer.MAX_VALUE;
		int tllatitude = 0;
		int brlongitude = 0;
		int brlatitude = Integer.MAX_VALUE;
		for (SIGNAL_LOC item : labelList)
		{
			tllongitude = item.longitude < tllongitude ? item.longitude : tllongitude;
			tllatitude = item.latitude > tllatitude ? item.latitude : tllatitude;
			brlongitude = item.longitude > brlongitude ? item.longitude : brlongitude;
			brlatitude = item.latitude < brlatitude ? item.latitude : brlatitude;
		}

		double dis = GisFunction.GetDistance(tllongitude, tllatitude, brlongitude, brlatitude);
		// int duration = labelList.get(labelList.size() -1).stime -
		// labelList.get(0).stime;
		if (dis <= 100)
		{
			return true;
		}
		return false;
	}

	public class TimeSpan
	{
		int stime;
		int etime;

		public int getDuration()
		{
			return etime - stime;
		}
	}

}
