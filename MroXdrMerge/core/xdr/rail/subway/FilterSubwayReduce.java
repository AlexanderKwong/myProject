package xdr.rail.subway;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import jan.com.hadoop.mapred.DataDealReducer;

public  class FilterSubwayReduce 
{
	public static class mmeReduce extends DataDealReducer<IntWritable, Text, NullWritable, Text>{
		
		@Override
		protected void setup(Reducer<IntWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			
			super.setup(context);
			
		}

		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> values,
				Reducer<IntWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException
		{
			
			super.reduce(arg0, values, context);
			Iterator<Text> it = values.iterator();
			while (it.hasNext())
			{
				Text value = it.next();
				context.write(NullWritable.get(), value);

			}
			
		}
	}
	
	

	
	
}
