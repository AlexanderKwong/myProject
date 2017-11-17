package xdr.lablefill;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import StructData.SIGNAL_LOC;
import StructData.StaticConfig;
import jan.util.GisFunction;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import xdr.lablefill.LabelDeal.TimeSpan;

public class TestTypeDeal
{
	private long imsi;
	private boolean isDDDriver;
	private Map<String, TimeSpan> userHomeCellMap = new HashMap<String, TimeSpan>();

	public TestTypeDeal(long imsi, boolean isDDDriver, Map<String, TimeSpan> userHomeCellMap)
	{
		this.imsi = imsi;
		this.isDDDriver = isDDDriver;
		this.userHomeCellMap = userHomeCellMap;
	}

	public long getImsi()
	{
		return imsi;
	}

	public void deal(List<? extends SIGNAL_LOC> xdrItemList)
	{
		if (isDDDriver)
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "find didi driver : " + imsi);

			for (int i = 0; i < xdrItemList.size(); ++i)
			{
				SIGNAL_LOC xitem = xdrItemList.get(i);

				if ((xitem.location == 3)
						|| ((xitem.location == 2 || xitem.location == 4 || xitem.location == 5 || xitem.location == 6) && (xitem.radius <= 100 && xitem.radius >= 0 && xitem.longitude > 0)
								&& (xitem.loctp.equals("wf") || xitem.loctp.equals("cl") || xitem.loctp.equals("ll") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll"))))
				{
					xitem.testType = StaticConfig.TestType_DT;

					//
					xitem.longitudeGL = xitem.longitude;
					xitem.latitudeGL = xitem.latitude;
					xitem.testTypeGL = xitem.testType;
					xitem.locationGL = xitem.location;
					xitem.distGL = xitem.dist;
					xitem.radiusGL = xitem.radius;
					xitem.loctpGL = xitem.loctp;
					// xitem.indoorGL = xitem.indoor;
					xitem.indoorGL = (int) xitem.mt_speed;
					xitem.lableGL = xitem.mt_label;
					xitem.loctimeGL = xitem.stime;

					SIGNAL_LOC jitem = null;
					// 往前找
					int curJ = i - 1;
					while (curJ >= 0)
					{
						jitem = xdrItemList.get(curJ);
						if (Math.abs(xitem.stime - jitem.stime) <= 10)
						{
							jitem.longitudeGL = xitem.longitude;
							jitem.latitudeGL = xitem.latitude;
							jitem.testTypeGL = StaticConfig.TestType_DT;

							jitem.locationGL = xitem.location;
							jitem.distGL = xitem.dist;
							jitem.radiusGL = xitem.radius;
							jitem.loctpGL = xitem.loctp;
							// jitem.indoorGL = xitem.indoor;
							jitem.indoorGL = (int) xitem.mt_speed;
							jitem.lableGL = xitem.mt_label;

							jitem.moveDirect = xitem.moveDirect;
							jitem.loctimeGL = xitem.loctimeGL;

							if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
							{
								jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
								if (jitem.distGL < 0)
								{
									jitem.testType = StaticConfig.TestType_ERROR;
								}
							}
						}
						else
						{
							break;
						}
						curJ--;
					}

					// 往后找
					curJ = i + 1;
					while (curJ < xdrItemList.size())
					{
						jitem = xdrItemList.get(curJ);
						if (Math.abs(xitem.stime - jitem.stime) <= 10)
						{
							jitem.longitudeGL = xitem.longitude;
							jitem.latitudeGL = xitem.latitude;
							jitem.testTypeGL = StaticConfig.TestType_DT;

							jitem.locationGL = xitem.location;
							jitem.distGL = xitem.dist;
							jitem.radiusGL = xitem.radius;
							jitem.loctpGL = xitem.loctp;
							// jitem.indoorGL = xitem.indoor;
							jitem.indoorGL = (int) xitem.mt_speed;
							jitem.lableGL = xitem.mt_label;

							jitem.moveDirect = xitem.moveDirect;
							jitem.loctimeGL = xitem.loctimeGL;

							if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
							{
								jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
								if (jitem.distGL < 0)
								{
									jitem.testType = StaticConfig.TestType_ERROR;
								}
							}

						}
						else
						{
							break;
						}
						curJ++;
					}
				}
			}
		}
		else // 如果发现没有dt的点，但是sdk为高速的用户，也需要打上dt的标签
		{
			int cqtTestStartPos = -1;
			int cqtTestEndPos = -1;

			for (int i = 0; i < xdrItemList.size(); i++)
			{
				SIGNAL_LOC xitem = xdrItemList.get(i);
				SIGNAL_LOC jitem;
				int curJ;

				if (((xitem.location == 2 || xitem.location == 4 || xitem.location == 5 ||xitem.location == 7) && xitem.radius <= 100 && xitem.radius >= 0 && xitem.longitude > 0)
						|| (xitem.location == 6 && xitem.radius <= 50 && xitem.radius >= 0 && xitem.longitude > 0)
						|| (xitem.mt_label.equals("esti_static") && xitem.location == 10 && xitem.loctp.equals("fp")))
				{
					if ((!xitem.mt_label.equals("unknow") && !xitem.mt_label.equals("static")))
					{
						cqtTestStartPos = -1;
						cqtTestEndPos = -1;
					}

					if (xitem.mt_label.equals("high") && (xitem.loctp.equals("wf") || xitem.loctp.equals("ll") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll"))
							)
					{
						// 高速点回填
						xitem.testType = StaticConfig.TestType_DT;

						xitem.longitudeGL = xitem.longitude;
						xitem.latitudeGL = xitem.latitude;
						xitem.testTypeGL = StaticConfig.TestType_DT;
						xitem.locationGL = xitem.location;
						xitem.distGL = xitem.dist;
						xitem.radiusGL = xitem.radius;
						xitem.loctpGL = xitem.loctp;
						xitem.indoorGL = (int) xitem.mt_speed;
						xitem.lableGL = xitem.mt_label;
						xitem.loctimeGL = xitem.stime;

						// 往前找
						curJ = i - 1;
						while (curJ >= 0)
						{
							jitem = xdrItemList.get(curJ);
							if (Math.abs(xitem.stime - jitem.stime) <= 10)
							{
								jitem.longitudeGL = xitem.longitude;
								jitem.latitudeGL = xitem.latitude;
								jitem.testTypeGL = StaticConfig.TestType_DT;

								jitem.locationGL = xitem.location;
								jitem.distGL = xitem.dist;
								jitem.radiusGL = xitem.radius;
								jitem.loctpGL = xitem.loctp;
								// jitem.indoorGL = xitem.indoor;
								jitem.indoorGL = (int) xitem.mt_speed;
								jitem.lableGL = xitem.mt_label;

								jitem.moveDirect = xitem.moveDirect;
								jitem.loctimeGL = xitem.loctimeGL;

								if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
								{
									jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
									if (jitem.distGL < 0)
									{
										jitem.testType = StaticConfig.TestType_ERROR;
									}
								}

							}
							else
							{
								break;
							}
							curJ--;
						}

						// 往后找
						curJ = i + 1;
						while (curJ < xdrItemList.size())
						{
							jitem = xdrItemList.get(curJ);
							if (Math.abs(xitem.stime - jitem.stime) <= 10)
							{
								jitem.longitudeGL = xitem.longitude;
								jitem.latitudeGL = xitem.latitude;
								jitem.testTypeGL = StaticConfig.TestType_DT;

								jitem.locationGL = xitem.location;
								jitem.distGL = xitem.dist;
								jitem.radiusGL = xitem.radius;
								jitem.loctpGL = xitem.loctp;
								jitem.indoorGL = (int) xitem.mt_speed;
								jitem.lableGL = xitem.mt_label;

								jitem.moveDirect = xitem.moveDirect;
								jitem.loctimeGL = xitem.loctimeGL;

								if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
								{
									jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
									if (jitem.distGL < 0)
									{
										jitem.testType = StaticConfig.TestType_ERROR;
									}
								}
							}
							else
							{
								break;
							}
							curJ++;
						}
					}
					else if (xitem.mt_label.equals("low") && (xitem.loctp.equals("wf") || xitem.loctp.equals("ll") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll")))
					{// 低速点回填
						xitem.testType = StaticConfig.TestType_DT_EX;

						xitem.longitudeGL = xitem.longitude;
						xitem.latitudeGL = xitem.latitude;
						xitem.testTypeGL = StaticConfig.TestType_DT_EX;
						xitem.locationGL = xitem.location;
						xitem.distGL = xitem.dist;
						xitem.radiusGL = xitem.radius;
						xitem.loctpGL = xitem.loctp;
						xitem.indoorGL = (int) xitem.mt_speed;
						xitem.lableGL = xitem.mt_label;
						xitem.loctimeGL = xitem.stime;

						// 往前找
						curJ = i - 1;
						while (curJ >= 0)
						{
							jitem = xdrItemList.get(curJ);
							if (Math.abs(xitem.stime - jitem.stime) <= 10)
							{
								jitem.longitudeGL = xitem.longitude;
								jitem.latitudeGL = xitem.latitude;
								jitem.testTypeGL = StaticConfig.TestType_DT_EX;

								jitem.locationGL = xitem.location;
								jitem.distGL = xitem.dist;
								jitem.radiusGL = xitem.radius;
								jitem.loctpGL = xitem.loctp;
								// jitem.indoorGL = xitem.indoor;
								jitem.indoorGL = (int) xitem.mt_speed;
								jitem.lableGL = xitem.mt_label;

								jitem.moveDirect = xitem.moveDirect;
								jitem.loctimeGL = xitem.loctimeGL;

								if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
								{
									jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
									if (jitem.distGL < 0)
									{
										jitem.testType = StaticConfig.TestType_ERROR;
									}
								}

							}
							else
							{
								break;
							}
							curJ--;
						}

						// 往后找
						curJ = i + 1;
						while (curJ < xdrItemList.size())
						{
							jitem = xdrItemList.get(curJ);
							if (Math.abs(xitem.stime - jitem.stime) <= 10)
							{
								jitem.longitudeGL = xitem.longitude;
								jitem.latitudeGL = xitem.latitude;
								jitem.testTypeGL = StaticConfig.TestType_DT_EX;

								jitem.locationGL = xitem.location;
								jitem.distGL = xitem.dist;
								jitem.radiusGL = xitem.radius;
								jitem.loctpGL = xitem.loctp;
								jitem.indoorGL = (int) xitem.mt_speed;
								jitem.lableGL = xitem.mt_label;

								jitem.moveDirect = xitem.moveDirect;
								jitem.loctimeGL = xitem.loctimeGL;

								if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
								{
									jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
									if (jitem.distGL < 0)
									{
										jitem.testType = StaticConfig.TestType_ERROR;
									}
								}

							}
							else
							{
								break;
							}
							curJ++;
						}
					}
					else if (xitem.radius <= 80 && xitem.radius >= 0 && xitem.mt_label.equals("static")
							&& (xitem.loctp.equals("ll") || xitem.loctp.equals("wf") || xitem.loctp.equals("ll2") || xitem.loctp.equals("lll")))
					{// 静态点回填
						xitem.testType = StaticConfig.TestType_CQT;

						xitem.longitudeGL = xitem.longitude;
						xitem.latitudeGL = xitem.latitude;
						xitem.testTypeGL = xitem.testType;

						xitem.locationGL = xitem.location;
						xitem.distGL = xitem.dist;
						xitem.radiusGL = xitem.radius;
						xitem.loctpGL = xitem.loctp;
						xitem.indoorGL = (int) xitem.mt_speed;
						xitem.lableGL = xitem.mt_label;
						xitem.loctimeGL = xitem.stime;

						if (cqtTestStartPos >= 0)
						{
							cqtTestEndPos = i;

							SIGNAL_LOC sItem = xdrItemList.get(cqtTestStartPos);
							SIGNAL_LOC eItem = xdrItemList.get(cqtTestEndPos);

							// 属于常驻小区才能进行回填
							TimeSpan timeSpan1 = userHomeCellMap.get(sItem.GetCellKey());
							TimeSpan timeSpan2 = userHomeCellMap.get(eItem.GetCellKey());
							// if((timeSpan1 != null && timeSpan1.getDuration()
							// >= 3600)
							// || (timeSpan2 != null && timeSpan2.getDuration()
							// >= 3600))
							// if(timeSpan1 != null || timeSpan2 != null ||
							// sItem.GetCellKey().equals(eItem.GetCellKey()))
							if (true)
							{
								double dis = GisFunction.GetDistance(sItem.longitude, sItem.latitude, eItem.longitude, eItem.latitude);

								if (dis <= 100)
								{
									for (int ii = cqtTestStartPos + 1; ii < cqtTestEndPos; ++ii)
									{
										jitem = xdrItemList.get(ii);

										if (jitem.longitude <= 0 && jitem.GetCellKey().equals(sItem.GetCellKey()))
										{
											jitem.longitudeGL = sItem.longitude;
											jitem.latitudeGL = sItem.latitude;
											jitem.testTypeGL = StaticConfig.TestType_CQT;

											jitem.locationGL = sItem.location;
											jitem.distGL = sItem.dist;
											jitem.radiusGL = sItem.radius;
											jitem.loctpGL = sItem.loctp;
											jitem.indoorGL = (int) sItem.mt_speed;
											jitem.lableGL = sItem.mt_label;
											jitem.loctimeGL = sItem.stime;

											if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
											{
												jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
												if (jitem.distGL < 0)
												{
													jitem.testType = StaticConfig.TestType_ERROR;
												}
											}
										}
										else if (jitem.longitude <= 0 && jitem.GetCellKey().equals(eItem.GetCellKey()))
										{
											jitem.longitudeGL = eItem.longitude;
											jitem.latitudeGL = eItem.latitude;
											jitem.testTypeGL = StaticConfig.TestType_CQT;

											jitem.locationGL = eItem.location;
											jitem.distGL = eItem.dist;
											jitem.radiusGL = eItem.radius;
											jitem.loctpGL = eItem.loctp;
											jitem.indoorGL = (int) eItem.mt_speed;
											jitem.lableGL = eItem.mt_label;
											jitem.loctimeGL = eItem.stime;

											if (!jitem.GetCellKey().equals(xitem.GetCellKey()))
											{
												jitem.distGL = jitem.GetSampleDistance(xitem.longitude, xitem.latitude);
												if (jitem.distGL < 0)
												{
													jitem.testType = StaticConfig.TestType_ERROR;
												}
											}
										}
									}
								}
								else
								{
									// 如果两个静止点相隔太远了，就说明用户这段时间不是静止
									sItem.testType = StaticConfig.TestType_DT_EX;
									sItem.testTypeGL = sItem.testType;
								}
							}
							else
							{
								// 如果开始和结束的小区都不属于常驻小区，就可以判断这个点属于慢速点
								sItem.testType = StaticConfig.TestType_DT_EX;
								sItem.testTypeGL = sItem.testType;
							}

							cqtTestStartPos = cqtTestEndPos;
							cqtTestEndPos = -1;
						}
						else
						{
							cqtTestStartPos = i;
							cqtTestEndPos = -1;
						}
					}
					else if (xitem.location == 10 && xitem.loctp.equals("fp") && xitem.mt_label.equals("esti_static"))
					{
						xitem.testType = StaticConfig.TestType_CQT;
						xitem.longitudeGL = xitem.longitude;
						xitem.latitudeGL = xitem.latitude;
						xitem.testTypeGL = xitem.testType;
						xitem.locationGL = xitem.location;
						xitem.distGL = xitem.dist;
						xitem.radiusGL = xitem.radius;
						xitem.loctpGL = xitem.loctp;
						xitem.indoorGL = (int) xitem.mt_speed;
						xitem.lableGL = xitem.mt_label;
						xitem.loctimeGL = xitem.stime;
					}
				}
			}
		}

		int maxRadius;
		for (int i = 0; i < xdrItemList.size(); i++)
		{
			SIGNAL_LOC tTemp = xdrItemList.get(i);

			// 去除有距离超远的数据
			maxRadius = tTemp.GetMaxCellRadius();
			if (tTemp.distGL >= maxRadius)
			{
				tTemp.testType = StaticConfig.TestType_ERROR;
				continue;
			}

			// 对于用于关联的采样点，测试类型和经纬度，都可以回填，用于用户的回放分析
			if (tTemp.testType != StaticConfig.TestType_DT && tTemp.testType != StaticConfig.TestType_CQT && tTemp.testType != StaticConfig.TestType_DT_EX)
			{
				if (tTemp.testTypeGL == StaticConfig.TestType_DT || tTemp.testTypeGL == StaticConfig.TestType_CQT || tTemp.testTypeGL == StaticConfig.TestType_DT_EX)
				{
					tTemp.longitude = tTemp.longitudeGL;
					tTemp.latitude = tTemp.latitudeGL;

					tTemp.testType = tTemp.testTypeGL;
					tTemp.location = tTemp.locationGL;
					tTemp.dist = tTemp.distGL;
					tTemp.radius = tTemp.radius;
					tTemp.loctp = tTemp.loctpGL;
					tTemp.indoor = StaticConfig.Int_Abnormal;
					tTemp.lable = tTemp.lableGL;
				}
			}

		}

	}

}
