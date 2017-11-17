package xdr.locallex;

import StructData.StaticConfig;

public class EventDataStruct
{
	public String strvalue[];
	public double fvalue[];
	
	public EventDataStruct()
	{
		strvalue = new String[2];
		for (int i = 0; i < strvalue.length; i++)
		{
			strvalue[i] = "";
		}
		
		fvalue = new double[20];
		for (int f = 0; f < fvalue.length; f++)
		{
			fvalue[f] = StaticConfig.Int_Abnormal;
		}
	}
	
	public void stat(EventDataStruct data)
	{	
		for (int i = 0; i < data.fvalue.length; i++)
		{
			if(data.fvalue[i] >0)
			{
				if(fvalue[i]<0){
					fvalue[i] = data.fvalue[i];
				}else{
					fvalue[i] += data.fvalue[i];
				}
			}
		}	

	}
	
	public void toString(StringBuffer sb)
	{

		
		for (int i = 0; i < fvalue.length; i++)
		{
			if(i ==  fvalue.length - 1)
			{
				sb.append(fvalue[i]);
			}
			else 
			{
				sb.append(fvalue[i]);sb.append("\t");
			}	
		}
	}
	
	public void toString(StringBuffer sb,int type)
	{
		for (int i = 0; i < strvalue.length; i++)
		{
			sb.append(strvalue[i]);sb.append("\t");
		}
		
		for (int i = 0; i < fvalue.length; i++)
		{
			if(i ==  fvalue.length - 1)
			{
				sb.append(fvalue[i]);;
			}
			else 
			{
				sb.append(fvalue[i]);sb.append("\t");
			}	
		}
	}
	
	
}
