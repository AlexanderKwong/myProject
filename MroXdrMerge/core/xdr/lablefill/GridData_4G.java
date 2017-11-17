package xdr.lablefill;

import StructData.DT_Sample_4G;
import StructData.Stat_Grid_4G;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.data.MyInt;
import util.LteStatHelper;

public class GridData_4G
{
	private int startTime;
	private int endTime;
	private Stat_Grid_4G lteGrid;

	public GridData_4G(int startTime, int endTime)
	{
		this.startTime = startTime;
		this.endTime = endTime;
		lteGrid = new Stat_Grid_4G();
	}

	public Stat_Grid_4G getLteGrid()
	{
		return lteGrid;
	}

	public int getStartTime()
	{
		return startTime;
	}

	public int getEndTime()
	{
		return endTime;
	}

	public void dealSample(DT_Sample_4G sample)
	{
		boolean isMroSample = sample.flag.toUpperCase().equals("MRO");
		boolean isMreSample = sample.flag.toUpperCase().equals("MRE");

		lteGrid.isamplenum++;
		if (isMroSample || isMreSample)
		{
			lteGrid.MrCount++;
			LteStatHelper.statMro(sample, lteGrid.tStat);
		}
		else
		{
			lteGrid.XdrCount++;
			lteGrid.iduration += sample.duration;
			LteStatHelper.statEvt(sample, lteGrid.tStat);

			// 只有xdr，才算用户的个数，mr不用算
			if (sample.IMSI > 0)
			{
				MyInt item = lteGrid.imsiMap.get(sample.IMSI);
				if (item == null)
				{
					item = new MyInt(0);
					lteGrid.imsiMap.put(sample.IMSI, item);
				}
				item.data++;
			}
		}
	}

	public void finalDeal()
	{
		lteGrid.UserCount_4G = lteGrid.imsiMap.size();
	}

}
