package xdr.locallex;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.MultiOutputMng;

public class EventDataBuildGridStat extends AStatDo
{
	private Map<String, EventDataBuildGrid> dataMap;
	private int stime;
	private int etime;
	private String resultTBName;
	
	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;

	public EventDataBuildGridStat(int stime, int etime, String resultTBName, MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.stime = stime;
		this.etime = etime;
		this.mosMng = mosMng;
		this.resultTBName = resultTBName;
		
		dataMap = new HashMap<String, EventDataBuildGrid>();
		sb = new StringBuffer();
		curText= new Text();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if(event.iBuildID <= 0)
		{
			return 0;
		}
		tmStr = event.iBuildID + "";
		EventDataBuildGrid item = dataMap.get(tmStr);
		if(item == null)
		{
			item = new EventDataBuildGrid(event.iCityID, event.iBuildID, stime);
			dataMap.put(tmStr, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataBuildGrid item : dataMap.values())
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
		
		dataMap = new HashMap<String, EventDataBuildGrid>();
		
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataBuildGrid item : dataMap.values())
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
		
		
		dataMap = new HashMap<String, EventDataBuildGrid>();	
		return 0;
	}
		
	
	
	public class EventDataBuildGrid extends EventDataStatDo
	{
	    protected int iCityID;
	    protected int iBuildingID;
	    protected int iTime;
	    
	    public EventDataBuildGrid(int iCityID, int iBuildingID, int iTime)
	    {
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iBuildingID = iBuildingID;	
	    	this.iTime = iTime;	
	    }


		@Override
		public int toString(StringBuffer sb)
		{
			int pos =0 ;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				sb.append(iCityID);sb.append("\t");
				sb.append(iBuildingID);sb.append("\t");
//				sb.append(iHeight);sb.append("\t");

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
