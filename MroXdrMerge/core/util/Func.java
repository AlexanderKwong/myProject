package util;

import StructData.StaticConfig;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class Func
{
	public static final int YYS_YiDong = 1;
	public static final int YYS_LianTong = 2;
	public static final int YYS_DianXin = 3;
	public static final int YYS_UnKnown = 4;

	public static boolean checkFreqIsLtDx(int freq)
	{
		if (freq < 30000 || freq == 40340)
		{
			return true;
		}
		return false;
	}

	/**
	 * 确定运营商
	 * 
	 * @param freq
	 *            频点
	 * @return
	 */
	public static int getFreqType(int freq)
	{
		switch (freq)
		{
		case 400:
		case 401:
		case 450:
		case 500:
		case 1500:
		case 1506:
		case 3770:
		case 1600:
		case 1650:
		case 40340:
			return YYS_LianTong;

		case 1775:
		case 1800:
		case 1825:
		case 1850:
		case 1870:
		case 75:
		case 100:
		case 2440:
		case 2446:
		case 2452:
		case 41140:
			return YYS_DianXin;

		default:
			if (freq >= 2410 && freq <= 2510)
			{// 800M
				return YYS_DianXin;
			}
			else if (freq >= 30000 && freq != 40340)
			{
				return YYS_YiDong;
			}
			return YYS_UnKnown;
		}
	}

	/**
	 * 根据loctp,确定 high mid low
	 * 
	 * @param loctp
	 * @return
	 */
	public static int getLocSource(String loctp)
	{
		if (loctp.equals("ll"))
		{
			return StaticConfig.LOCTYPE_HIGH;
		}
		else if (loctp.equals("wf"))// uri定位
		{
			return StaticConfig.LOCTYPE_MID;
		}
		else if (loctp.contains("fp") || loctp.equals("cl"))
		{
			return StaticConfig.LOCTYPE_LOW;
		}
		return 0;
	}

	/**
	 * 经度元整
	 * 
	 */
	public static long getRoundLongtitude(int size, long longtitude)
	{
		return longtitude / (100 * size) * (100 * size);
	}

	public static long getRoundLongtitude(int size, String longtitude)
	{
		long longtitudetemp = Long.parseLong(longtitude);
		return longtitudetemp / (100 * size) * (100 * size);
	}

	/**
	 * 右下经度
	 * 
	 */
	public static long getRoundBRLongtitude(int size, long longtitude)
	{
		return longtitude / (100 * size) * (100 * size) + (100 * size);
	}

	public static long getRoundBRLongtitude(int size, String longtitude)
	{
		long longtitudetemp = Long.parseLong(longtitude);
		return longtitudetemp / (100 * size) * (100 * size) + (100 * size);
	}

	/**
	 * 纬度元整
	 * 
	 */
	public static long getRoundLatitude(int size, long latitude)
	{
		return latitude / (90 * size) * (90 * size);
	}

	public static long getRoundLatitude(int size, String latitude)
	{
		long latitudetemp = Long.parseLong(latitude);
		return latitudetemp / (90 * size) * (90 * size);
	}

	/**
	 * 左上纬度
	 * 
	 */
	public static long getRoundTLLatitude(int size, long latitude)
	{
		return latitude / (90 * size) * (90 * size) + (90 * size);
	}

	public static long getRoundTLLatitude(int size, String latitude)
	{
		long latitudetemp = Long.parseLong(latitude);
		return latitudetemp / (90 * size) * (90 * size) + (90 * size);
	}

	public static int getSampleConfidentType(String loctp, int samState, int testType)
	{
		if (samState == StaticConfig.ACTTYPE_IN && loctp.equals("ll"))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IH;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && loctp.equals("wf"))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && (loctp.contains("fp") || loctp.equals("cl")))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && loctp.contains("ll"))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OH;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OM;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && loctp.contains("wf"))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && (loctp.contains("fp") || loctp.equals("cl")))
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		return 0;

	}

	public static String getEncryptIMSI(String IMSI)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EncryptUser))
		{
			return StringUtil.EncryptStringToLong(IMSI) + "";
		}
		return IMSI;
	}

	public static String getEncryptMSISDN(String MSISDN)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EncryptUser))
		{
			return StringUtil.EncryptStringToLong(MSISDN) + "";
		}
		return MSISDN;
	}

	public static int getSampleConfidentType(int locSource, int samState, int testType)
	{
		if (samState == StaticConfig.ACTTYPE_IN && locSource == StaticConfig.LOCTYPE_HIGH)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IH;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && locSource == StaticConfig.LOCTYPE_MID)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_IN && locSource == StaticConfig.LOCTYPE_LOW)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.IL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.IL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && locSource == StaticConfig.LOCTYPE_HIGH)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OH;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OM;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && locSource == StaticConfig.LOCTYPE_MID)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OM;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		else if (samState == StaticConfig.ACTTYPE_OUT && locSource == StaticConfig.LOCTYPE_LOW)
		{
			if (testType == StaticConfig.TestType_DT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_CQT)
			{
				return StaticConfig.OL;
			}
			else if (testType == StaticConfig.TestType_DT_EX)
			{
				return StaticConfig.OL;
			}
		}
		return 0;
	}

	/**
	 * 根据location ，确定 high mid low
	 * 
	 * @param location
	 * @return
	 */
	public static int getLocSource(int location)
	{
		if (location == 2)// uri定位
		{
			return StaticConfig.LOCTYPE_MID;
		}
		return 0;
	}
}
