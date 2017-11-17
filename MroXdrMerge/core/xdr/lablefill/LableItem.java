package xdr.lablefill;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LableItem
{
	public long imsi;
	public int begin_time;
	public int end_time;
	public String lable;

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Date d_beginTime;
	private String strTime;

	public boolean FillData(String[] vals, int startPos)
	{
		int i = startPos;
		imsi = Long.parseLong(vals[i++]);

		strTime = vals[i++];
		try
		{
			strTime = strTime.substring(0, strTime.length()-7);
		    d_beginTime = format.parse(strTime);
		    begin_time = (int) (d_beginTime.getTime() / 1000L);
		}
		catch (Exception e)
		{
			return false;
		}
		
		strTime = vals[i++];
		try
		{
			strTime = strTime.substring(0, strTime.length()-7);
		    d_beginTime = format.parse(strTime);
		    end_time = (int) (d_beginTime.getTime() / 1000L);
		}
		catch (Exception e)
		{
			return false;
		}	

		lable = vals[i++];

		return true;
	}
}
