package xdr.locallex;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;

public class EventDataCellStat extends AStatDo
{
	private Map<Integer, EventDataCell> dataMap;
	private int stime;
	private int etime;
	private String resultTBName;
	
	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText;
	private StringBuffer sb;

	public EventDataCellStat(int stime, int etime, String resultTBName, MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.stime = stime;
		this.etime = etime;
		this.mosMng = mosMng;
		this.resultTBName = resultTBName;
		curText = new Text();
		dataMap = new HashMap<Integer, EventDataCell>();
		sb = new StringBuffer();
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;
		
		if(event.iEci <= 0)
		{
			return 0;
		}
		EventDataCell item = dataMap.get(event.iEci);
		if(item == null)
		{
			item = new EventDataCell(event.iCityID, event.iEci, stime);
			dataMap.put(item.iECI, item);
		}
		item.stat(event);		
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataCell item : dataMap.values())
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
				LOGHelper.GetLogger().writeLog(LogType.error, "EventDataCellStat: outFinalReusltSub Exception: "+e.getMessage());
				// TODO: handle exception
			}
		}
		return 0;
	}
      	
	public class EventDataCell extends EventDataStatDo
	{
	    protected int iCityID;
	    protected int iECI;
	    protected int iTime;
	    
	    public EventDataCell(int iCityID, int iECI, int iTime)
	    {	
	    	super();
	    	
	    	this.iCityID = iCityID;
	    	this.iECI = iECI;
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
				sb.append(iECI);sb.append("\t");
				sb.append(statModelEntry.getKey().getInterface());sb.append("\t");
				sb.append(statModelEntry.getKey().getKpiset());sb.append("\t");
				sb.append(iTime);sb.append("\t");
//				sb.append(statModelEntry.getKey().getProcedureType());sb.append("\t");
				statModelEntry.getValue().toString(sb);		
				if(pos<statModelMap.size()){
					sb.append("\n");			
				}
			}
			return 0;
		}
	      
	      
	}

}
