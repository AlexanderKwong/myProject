package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import jan.util.DataAdapterReader;
import jan.util.DataAdapterConf.ParseItem;
import xdr.locallex.EventData;

public abstract class XdrDataBase implements Comparable<XdrDataBase>
{
    @Override
	public int compareTo(XdrDataBase o)
	{
		double a = this.getIstime() + this.getIstimems() / 1000.0;
		double b = o.getIstime() + o.getIstimems() / 1000.0;

		if (a > b)
		{
			return 1;
		}
		else if (a < b)
		{
			return -1;
		}
		else
		{
			return 0;
		}
	}

	private int dataType;
	
   
	public long imsi;
	public int istime;
	public int istimems;
	public int ietime;
	public int ietimems;
	
	public int iCityID;
	public int iLongitude;
	public int iLatitude;
	public int iDoorType;
	public int iAreaType;
	public int iAreaID;
	public String strloctp;
	public int iRadius;

	public int ibuildid;
	public int iheight;
	public int testType;
	public String label;
	public int locSource;

	private String srcData;
	
	public int LteScRSRP;
	public int LteScSinrUL;
	
	public int confidentType;
	
	public long s1apid;
	public long ecgi;
	
	public XdrDataBase()
	{
		dataType = -1;
		
		imsi = -1;
		istime = -1;
		ietime = -1;
		iLongitude = -1;
		iLatitude = -1;
		iDoorType = -1;
		iAreaType = -1;
		iAreaID = -1;
		strloctp = "";
		iRadius = -1;

		srcData = "";
		
		LteScRSRP = -1000000;
		LteScSinrUL = -1000000;
		
		confidentType = -1;
	}

	public long getImsi()
	{
		return imsi;
	}

	public int getIstime()
	{
		return istime;
	}
	
	public int getIstimems()
	{
		return istimems;
	}

	public int getIetime()
	{
		return ietime;
	}
	
	public int getIetimems()
	{
		return ietimems;
	}
	
	public int getDataType()
	{
		return dataType;
	}
	
	public void setDataType(int dataType)
	{
		this.dataType = dataType;
	}
	
	
	
	public long getS1apid()
	{
		return s1apid;
	}

	public void setS1apid(long s1apid)
	{
		this.s1apid = s1apid;
	}

	public long getEcgi()
	{
		return ecgi;
	}

	public void setEcgi(long ecgi)
	{
		this.ecgi = ecgi;
	}

	public abstract ParseItem getDataParseItem() throws IOException;

	public abstract boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException;
	
	public abstract boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException;
	
    public abstract ArrayList<EventData> toEventData();
    
    public abstract void toString(StringBuffer sb);
    
}
