package xdr.lablefill;

import StructData.DT_Sample_4G;
import StructData.NC_LTE;
import StructData.Stat_Cell_4G;
import StructData.StaticConfig;
import util.LteStatHelper;

public class CellData_4G
{
	private int lac;
	private long eci;
	private int startTime;
	private int endTime;
	private Stat_Cell_4G lteCell;

	public CellData_4G(int cityID, int lac, long eci, int startTime, int endTime)
	{
		this.lac = lac;
		this.eci = eci;
		this.startTime = startTime;
		this.endTime = endTime;

		lteCell = new Stat_Cell_4G();
		lteCell.Clear();

		lteCell.icityid = cityID;
		lteCell.startTime = startTime;
		lteCell.endTime = endTime;
		lteCell.iLAC = lac;
		lteCell.wRAC = 0;
		lteCell.iCI = eci;
	}

	public int getLac()
	{
		return lac;
	}

	public long getEci()
	{
		return eci;
	}

	public Stat_Cell_4G getLteCell()
	{
		return lteCell;
	}

	public void dealSample(DT_Sample_4G sample)
	{
		boolean isSampleMro = sample.flag.toUpperCase().equals("MRO");
		boolean isSampleMre = sample.flag.toUpperCase().equals("MRE");

		// 小区统计
		lteCell.iduration += sample.duration;
		if (isSampleMro || isSampleMre)
		{
			lteCell.isamplenum++;
			LteStatHelper.statMro(sample, lteCell.tStat);

			int result = isSampleJam(sample);
			if (result == 1 || result == 2)
			{
				lteCell.sfcnJamSamCount++;
			}

			if (result == 2 || result == 3)
			{
				lteCell.sdfcnJamSamCount++;
			}

			if (isSampleMro)
			{
				lteCell.mroCount++;
				if (sample.IMSI > 0)
				{
					lteCell.mroxdrCount++;
				}
			}
			else if (isSampleMre)
			{
				lteCell.mreCount++;
				if (sample.IMSI > 0)
				{
					lteCell.mrexdrCount++;
				}
			}
		}
		else
		{
			lteCell.xdrCount++;

			if (sample.ilongitude > 0 && sample.isOriginalLoction())
			{
				lteCell.totalLocXdrCount++;

				if (sample.loctp.equals("ll") || sample.loctp.equals("ll2") || sample.loctp.equals("wf") && sample.radius <= 100 && sample.radius >= 0)
				{
					lteCell.validLocXdrCount++;
				}

				if (sample.testType == StaticConfig.TestType_DT)
				{
					lteCell.dtXdrCount++;
				}
				else if (sample.testType == StaticConfig.TestType_CQT)
				{
					lteCell.cqtXdrCount++;
				}
				else if (sample.testType == StaticConfig.TestType_DT_EX)
				{
					lteCell.dtexXdrCount++;
				}
			}

			LteStatHelper.statEvt(sample, lteCell.tStat);
		}

	}

	public int isSampleJam(DT_Sample_4G tsam)
	{
		int sfcnJamCellCount = 0;
		int dfcnJamCellCount = 0;

		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						sfcnJamCellCount++;
					}
					else
					{
						dfcnJamCellCount++;
					}
				}
			}
		}

		int result = 0;
		if (sfcnJamCellCount >= 3)
		{
			result = 1;
		}

		if (sfcnJamCellCount + dfcnJamCellCount >= 3)
		{
			if (result == 1)
			{
				result = 2;
			}
			else
			{
				result = 3;
			}

		}

		return result;
	}

}
