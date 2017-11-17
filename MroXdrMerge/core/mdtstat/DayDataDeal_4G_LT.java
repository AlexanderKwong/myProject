package mdtstat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import jan.util.IWriteLogCallBack;
import jan.util.LOGHelper;
import jan.util.TimeHelper;
import mrstat.AStatDo;
import mrstat.TypeResult;

public class DayDataDeal_4G_LT
{
	private Map<Integer, DayDataItem> dayDataDealMap;// time，天统计
	private int curDayTime;
	private TypeResult typeResult;

	public DayDataDeal_4G_LT(TypeResult typeResult)
	{
		dayDataDealMap = new HashMap<Integer, DayDataItem>();
		this.typeResult = typeResult;
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}

		curDayTime = TimeHelper.getRoundDayTime(sample.itime);
		DayDataItem dayDataDeal = dayDataDealMap.get(curDayTime);
		if (dayDataDeal == null)
		{
			dayDataDeal = new DayDataItem(typeResult, curDayTime, curDayTime + 86400);
			dayDataDealMap.put(curDayTime, dayDataDeal);
		}
		dayDataDeal.dealMr(sample);
	}

	public Map<Integer, DayDataItem> getDayDataDealMap()
	{
		return dayDataDealMap;
	}

	public int outResult()
	{
		for (DayDataItem item : dayDataDealMap.values())
		{
			item.outResult();
		}
		return 0;
	}

	public class DayDataItem
	{
		private int stime;
		private int etime;
		private TypeResult typeResult;

		private List<AStatDo> statdoList = new ArrayList<AStatDo>();

		public DayDataItem(TypeResult typeResult, int stime, int etime)
		{
			this.stime = stime;
			this.etime = etime;
			this.typeResult = typeResult;

			statdoList.add(new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT));
			statdoList.add(new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT));
			statdoList.add(new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_LT));
			statdoList.add(new CellStatDO_4G(typeResult, StaticConfig.SOURCE_LT));

		}

		public void outResult()
		{
			for (AStatDo item : statdoList)
			{
				if (item.outFinalReuslt() != 0)
				{
					LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "outFinalReuslt error: " + item.getClass().toString());
				}
			}
		}

		public void dealMr(DT_Sample_4G sample)
		{
			for (AStatDo item : statdoList)
			{
				item.stat(sample);
			}
		}

		public void dealEvent(DT_Sample_4G sample)
		{
			for (AStatDo item : statdoList)
			{
				item.stat(sample);
			}
		}
	}

}
