package xdr.locallex;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;
import util.Func;

public class EventDataAreaCellGridStat extends AStatDo
{
	private Map<String, EventDataAreaCellGrid> dataMap;
	private int stime;
	private int etime;
	private String resultTBName;

	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText;
	private StringBuffer sb;
	private String tmStr;

	public EventDataAreaCellGridStat(int stime, int etime, String resultTBName, MultiOutputMng<NullWritable, Text> mosMng)
	{
		this.stime = stime;
		this.etime = etime;
		this.mosMng = mosMng;
		this.resultTBName = resultTBName;
		curText = new Text();
		dataMap = new HashMap<String, EventDataAreaCellGrid>();
		sb = new StringBuffer();
		
	}

	@Override
	public int statSub(Object o)
	{
		EventData event = (EventData) o;

		if (event.iEci <= 0)
		{
			return 0;
		}

		/**
		 * TODO zhaikaishun 2017-09-26 [2017-09-26]
		 * iBuildID到底是否需要，我觉得不需要判断，我先暂时注释掉
		 */
//		if (event.iBuildID > 0)
//		{
//			return 0;
//		}
		
		tmStr = event.iCityID + "," + event.iAreaType + "," + event.iAreaID+","+event.gridItem.tllongitude+","
		+event.gridItem.tllatitude +","+event.iEci+","+stime;
		EventDataAreaCellGrid item = dataMap.get(tmStr);
		if (item == null)
		{
			item = new EventDataAreaCellGrid(event.iCityID, event.gridItem.tllongitude,
					event.gridItem.tllatitude,event.gridItem.brlongitude,event.gridItem.brlatitude,
					event.iEci, stime,event.iAreaType,event.iAreaID);
			dataMap.put(tmStr, item);
		}
		item.stat(event);
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		for (EventDataAreaCellGrid item : dataMap.values())
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

		dataMap = new HashMap<String, EventDataAreaCellGrid>();

		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (EventDataAreaCellGrid item : dataMap.values())
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

		dataMap = new HashMap<String, EventDataAreaCellGrid>();
		return 0;
	}

	public class EventDataAreaCellGrid extends EventDataStatDo
	{
		protected int iCityID;
		protected int iTLlongitude;
		protected int iTLlatitude;
		protected int iBRlongitude;
		protected int iBRlatitude;
		protected int iECI;
		protected int iTime;
		
		protected int iAreatype;
		protected int iAreaID;

		public EventDataAreaCellGrid(int iCityID, int iLongitude, int iLatitude,int iBRlongitude ,
				int iBRlatitude , int iECI, int iTime,int iAreatype,int iAreaID)
		{
			super();

			this.iCityID = iCityID;
			this.iTLlongitude = iLongitude;
			this.iTLlatitude = iLatitude;
			this.iBRlongitude = iBRlongitude;
			this.iBRlatitude = iBRlatitude;
			this.iECI = iECI;
			this.iTime = iTime;
			this.iAreatype = iAreatype;
			this.iAreaID = iAreaID;
		}

		@Override
		public int toString(StringBuffer sb)
		{
			int pos = 0;
			for (Map.Entry<EventDataStatKey, EventDataStruct> statModelEntry : statModelMap.entrySet())
			{
				pos++;
				
				sb.append(iCityID);sb.append("\t");
				sb.append(iAreatype);sb.append("\t");
				sb.append(iAreaID);sb.append("\t");
				sb.append(iTLlongitude);sb.append("\t");
				sb.append(iTLlatitude);sb.append("\t");
				sb.append(iBRlongitude);sb.append("\t");
				sb.append(iBRlatitude);sb.append("\t");
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
