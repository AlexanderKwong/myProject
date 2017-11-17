package xdr.locallex;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.MultiOutputMng;

public class DataStater
{
	private DayDataDeal_4G dayDataDeal_4G;
    
    public DataStater(MultiOutputMng<NullWritable, Text> mosMng)
    {
    	dayDataDeal_4G = new DayDataDeal_4G(mosMng);
    	 
    }
	
	public int stat(EventData eventData)
	{
		dayDataDeal_4G.dealEvent(eventData);
		return 0;
	}
	
	public int outResult()
	{
		// ALL
		dayDataDeal_4G.outResult();

		return 0;
	}
	
	
}
