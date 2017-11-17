package xdr.locallex;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;
import util.Func;

public class EventDataOutGridStat extends AStatDo
{
	private Map<String, EventDataOutGrid> dataMap;
	private int stime;
	private int etime;
	private String resultTBName;
		
	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;
	
	public EventDataOutGridStat(int stime, int etime, String resultTBName, MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.stime = stime;
		this.etime = etime;
		this.mosMng = mosMng;
		this.resultTBName = resultTBName;
		curText= new Text();
		dataMap = new HashMap<String, EventDataOutGrid>();
		sb = new StringBuffer();
		
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if(event.iBuildID > 0)
		{
			return 0;
		}
		
		tmStr =event.gridItem.tllongitude + "," + event.gridItem.tllatitude;
		EventDataOutGrid item = dataMap.get(tmStr);
		if(item == null)
		{
			item = new EventDataOutGrid(event.iCityID, event.gridItem.tllongitude, 
					event.gridItem.tllatitude,event.gridItem.brlongitude,event.gridItem.brlatitude, stime);
			dataMap.put(tmStr, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataOutGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				curText.set(sb.toString());
				mosMng.write(resultTBName, NullWritable.get(), curText);
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}
		}
		
		dataMap = new HashMap<String, EventDataOutGrid>();
		
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataOutGrid item : dataMap.values())
		{
			try
			{
				sb.delete(0, sb.length());
				item.toString(sb);
				curText.set(sb.toString());
				mosMng.write(resultTBName, NullWritable.get(), curText);
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}
		}
		
		
		dataMap = new HashMap<String, EventDataOutGrid>();	
		return 0;
	}
	
	
	
	
	
	public class EventDataOutGrid extends EventDataStatDo
	{
	    protected int iCityID;
	    protected int iLongitude;
	    protected int iLatitude;
	    protected int iTime;
	    protected int iBRlongitude;
	    protected int iBRlatitude;
	    
	    public EventDataOutGrid(int iCityID, int iLongitude, int iLatitude,int iBRlongitude,int iBRlatitude, int iTime)
	    {
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iLongitude = iLongitude;
	    	this.iLatitude = iLatitude;
	    	this.iBRlongitude = iBRlongitude;
	    	this.iBRlatitude = iBRlatitude;
	    	this.iTime = iTime; 	
	    }


		@Override
		public int toString(StringBuffer sb)
		{
			int pos =0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);sb.append("\t");
				sb.append(iLongitude);sb.append("\t");
				sb.append(iLatitude);sb.append("\t");
				sb.append(iBRlongitude);sb.append("\t");
				sb.append(iBRlatitude);sb.append("\t");
				sb.append(statModelEntry.getKey().getInterface());sb.append("\t");
				sb.append(statModelEntry.getKey().getKpiset());sb.append("\t");
				sb.append(iTime);sb.append("\t");
				statModelEntry.getValue().toString(sb);	
				if(pos<statModelMap.size()){
					sb.append("\n");			
				}
				
			}
			return 0;
		}
	      
	      
	}
      
      
}
