package xdr.locallex;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.MultiOutputMng;

public class EventDataBuildCellGridStat extends AStatDo
{
	private Map<String, EventDataBuildCellGrid> dataMap;
	private int stime;
	private int etime;
	private String resultTBName;
	
	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;

	public EventDataBuildCellGridStat(int stime, int etime, String resultTBName, MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.stime = stime;
		this.etime = etime;
		this.mosMng = mosMng;
		this.resultTBName = resultTBName;
		
		dataMap = new HashMap<String, EventDataBuildCellGrid>();
		sb = new StringBuffer();
		curText= new Text();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if(event.iBuildID <= 0 || event.iEci <= 0)
		{
			return 0;
		}
		tmStr = event.iEci + "," + event.iBuildID;
		EventDataBuildCellGrid item = dataMap.get(tmStr);
		if(item == null)
		{
			item = new EventDataBuildCellGrid(event.iCityID, event.iBuildID, event.iHeight, event.iEci, stime);
			dataMap.put(tmStr, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataBuildCellGrid item : dataMap.values())
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
		
		dataMap = new HashMap<String, EventDataBuildCellGrid>();
		
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataBuildCellGrid item : dataMap.values())
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
		
		
		dataMap = new HashMap<String, EventDataBuildCellGrid>();	
		return 0;
	}
	
	
	
	
	public class EventDataBuildCellGrid extends EventDataStatDo
	{
	    protected int iCityID;
	    protected int iBuildingID;
	    protected int iHeight;
	    protected int iECI;
	    protected int iTime;
	    
	    public EventDataBuildCellGrid(int iCityID, int iBuildingID, int iHeight, int iECI, int iTime)
	    {
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iBuildingID = iBuildingID;
	    	this.iHeight = iHeight;	
	    	this.iECI = iECI;	
	    	this.iTime = iTime;	
	    }


		@Override
		public int toString(StringBuffer sb)
		{
			int pos = 0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
		
				sb.append(iCityID);sb.append("\t");
				sb.append(iBuildingID);sb.append("\t");
//				sb.append(iHeight);sb.append("\t");
				sb.append(iECI);sb.append("\t");
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
