package xdr.msisdn_imsi;

import java.io.IOException;

import jan.com.hadoop.mapred.DataDealReducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MsisdnJoinReducer
{
	public static class MsisdnReducer extends DataDealReducer<MsisdnKey, Text, NullWritable, Text>
	{
		Text curText = new Text();;

		MsisdnImsiModel msisdnImsiModel = null;
		
		@Override
		protected void setup(Reducer<MsisdnKey, Text, NullWritable, Text>.Context context) throws IOException,
				InterruptedException
		{
			super.setup(context); 
			curText = new Text();
		}

		@Override
		protected void reduce(MsisdnKey key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			
			if (key.getDataType() == 1)
			{
				msisdnImsiModel = new MsisdnImsiModel();
				msisdnImsiModel.setMsisdn(key.getMsisdn());
				if (msisdnImsiModel != null)
				{
					for (Text tempLoc : values)
					{
						msisdnImsiModel.setImsi(Long.parseLong(tempLoc.toString()));
					}
				}

			}			
			else if (key.getDataType() == 100)
			{
				if(msisdnImsiModel!=null && key.getMsisdn()==msisdnImsiModel.getMsisdn()){
					long imsi = msisdnImsiModel.getImsi();
					for (Text tempLoc : values){
						curText.set(tempLoc+","+imsi);
						context.write(NullWritable.get(), curText);
					}
				}
			}

		}
	}
}
