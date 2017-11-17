package mdtstat;

public class Util
{

	public static int RoundTimeForHour(int itime)
	{
		return (itime + 8 * 3600) / 3600 * 3600 - 8 * 3600;
	}

	public static int getRoundDayTime(int time)
	{
		return (time + 8 * 3600) / 86400 * 86400 - 8 * 3600;
	}

	public static int getHour(int time)
	{
		return (time + 8 * 3600) % 86400 / 3600;// 需要加上8小时，否则会比实际时间少8小时
	}

	public static void main(String args[])
	{
		System.out.println(getHour(1508317069));
	}

	public static int mdtType(String mrtype)
	{
		return mrtype.equals("MDT_IMM") ? 0 : 1;
	}

}
