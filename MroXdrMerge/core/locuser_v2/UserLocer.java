package locuser_v2;

import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;

public class UserLocer
{
	public ReportProgress rptProgress = new ReportProgress();

	public CfgInfo cInfo = new CfgInfo();
	public CellPorp cPorp = new CellPorp();
	public SamScsp sScsp = new SamScsp();
	public BuildAna buildAna = new BuildAna();
	public SamLocs sLocs = new SamLocs();

	public DataUnit dataUnit = new DataUnit();

	public UserLocer()
	{
	}

	public void DoWork(int nType, List<SIGNAL_MR_All> lSams, MultiOutputMng<NullWritable, Text> mosMng)
	{
		int OutSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOutSize());
		int InSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuInSize());
		int OrgOutSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOrgOutSize());
		int OrgInSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOrgInSize());
		if (OutSize < OrgOutSize || InSize < OrgInSize)
		{
			rptProgress.writeLog(0, "栅格尺寸OutSize,InSize,OrgOutSize,OrgInSize配置错误。");
			return;
		}
		if (OrgOutSize == 0)
		{
			OrgOutSize = OutSize;
		}
		if (OrgInSize == 0)
		{
			OrgInSize = InSize;
		}

		// 1.读取配置
		if (!cInfo.Init(rptProgress))
		{
			rptProgress.writeLog(0, "配置参数错误。");
			return;
		}

		dataUnit.Clear();

		// 2.读取采样点
		SamFiles sf = new SamFiles(rptProgress);

		if (sf.GetNext(dataUnit, lSams))
		{
			if (dataUnit.siCount() == 0)
			{
				return;
			}

			// 3.识别相关服务小区和邻区属性
			SetCell();
			// 4.构造session和切片
			GetScsp();
			// 用于做室内分析 by lanjiancai
			DoBuildAna(mosMng);
			// 5.定位
			SetLocs();
			// 6.输出
			OutPut(nType);

			dataUnit.Clear();
		}
	}

	private void SetCell()
	{
		rptProgress.writeLog(0, "小区属性...");

		cPorp.SetCell(cInfo, dataUnit);
	}

	private void GetScsp()
	{
		rptProgress.writeLog(0, "段落分析...");

		sScsp.GetScsp(dataUnit, rptProgress);
	}

	private void DoBuildAna(MultiOutputMng<NullWritable, Text> mosMng)
	{
		rptProgress.writeLog(0, "室分问题分析...");
		try
		{
			Text curText = new Text();
			for (IndoorErrResult temp : buildAna.AnaBuild(dataUnit, rptProgress).values())
			{
				curText.set(temp.toString());
				mosMng.write("indoorErr", NullWritable.get(), curText);
			}
		}
		catch (Exception e)
		{
		}
	}

	private void SetLocs()
	{
		rptProgress.writeLog(0, "开始定位...");

		try
		{
			sLocs.SetLocs(dataUnit, cInfo, rptProgress);
		}
		catch (Exception ee)
		{
		}
	}

	private void OutPut(int nType)
	{
		rptProgress.writeLog(0, "开始生成...");

		long spCount = dataUnit.spCount();
		if (spCount == 0)
		{
			return;
		}

		int OutSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOutSize());
		int InSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuInSize());
		int OrgOutSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOrgOutSize());
		int OrgInSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOrgInSize());
		int OutTimes = OutSize / OrgOutSize;
		int InTimes = InSize / OrgInSize;

		int ilong = 0;
		int ilati = 0;
		try
		{
			for (EciUnit eunit : dataUnit.eciUnits.values())
			{
				for (MrSplice sl : eunit.splices)
				{
					if (!eunit.samples.containsKey(sl.s1apid))
					{
						continue;
					}

					Random outrd = new Random();
					Random inrd = new Random();

					List<MrSam> msl = eunit.samples.get(sl.s1apid);

					for (MrSam ms : msl)
					{
						if (ms.itime > sl.splice_etime || ms.itime < sl.splice_btime)
						{
							continue;
						}

						if (sl.buildingid > 0)
						{
							if (InTimes == 1)
							{
								ilong = sl.longitude;
								ilati = sl.latitude;
							}
							else
							{
								int rd = inrd.nextInt(InTimes * InTimes);
								ilong = RandPos(sl.longitude, 100, InSize, OrgInSize, rd % InTimes, InTimes);
								ilati = RandPos(sl.latitude, 90, InSize, OrgInSize, rd / InTimes, InTimes);
							}
						}
						else
						{
							if (OutTimes == 1)
							{
								ilong = sl.longitude;
								ilati = sl.latitude;
							}
							else
							{
								int rd = outrd.nextInt(OutTimes * OutTimes);
								ilong = RandPos(sl.longitude, 100, OutSize, OrgOutSize, rd % OutTimes, OutTimes);
								ilati = RandPos(sl.latitude, 90, OutSize, OrgOutSize, rd / OutTimes, OutTimes);
							}
						}

						// 没有的才赋值
						if (ms.mall.tsc.longitude <= 0)
						{
							ms.mall.ispeed = sl.buildingid;
							ms.mall.imode = (short) sl.ilevel;
							ms.mall.tsc.longitude = ilong;
							ms.mall.tsc.latitude = ilati;
							ms.mall.locSource = StaticConfig.LOCTYPE_LOW;
							if ((sl.isroad & FingerGrid.HITYPE) > 0)
							{
								if (sl.buildingid > 0)
								{
									ms.mall.loctp = "fg7";
								}
								else
								{
									ms.mall.loctp = "fg6";
								}
							}
							else
							{
								ms.mall.loctp = "fg8";
							}
							ms.mall.simuLongitude = sl.longitude;
							ms.mall.simuLatitude = sl.latitude;
							ms.mall.indoor = sl.doortype; // 1室内2室外
							ms.mall.loctp = "fp" + String.valueOf(sl.loctype); // 1小区
																				// 2simu
							if (nType == 1)
							{
								if (sl.doortype == 1)
								{
									ms.mall.testType = StaticConfig.TestType_CQT;
								}
								else if (sl.doortype == 2)
								{
									ms.mall.testType = StaticConfig.TestType_DT;
								}
							}
						}
						if (sl.doortype == 1 || sl.doortype == 3)
						{
							ms.mall.samState = StaticConfig.ACTTYPE_IN;
						}
						else if (sl.doortype == 2 || sl.doortype == 4)
						{
							ms.mall.samState = StaticConfig.ACTTYPE_OUT;
						}
					}
				}
			}
		}
		catch (Exception ee)
		{
		}

		rptProgress.writeLog(0, "生成完毕。");
	}

	private int RandPos(int ipos, int nspan, int nsize, int osize, int rd, int ntimes)
	{
		int ilola = ipos / (nsize * nspan) * (nsize * nspan) + rd * (osize * nspan) + (osize * nspan) / 2;

		return ilola;
	}
}
