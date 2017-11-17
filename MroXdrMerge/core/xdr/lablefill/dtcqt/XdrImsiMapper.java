package xdr.lablefill.dtcqt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import StructData.NoTypeSignal;
import xdr.lablefill.by23g.ImsiTimeKey;

public class XdrImsiMapper
{
	
	public static class ImsiToDataMapper extends Mapper<LongWritable, Text, ImsiTimeKey, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			// value 提取出 ImsiTimeKey
			String[] strs = value.toString().split("\t");
			String imsi = "";
			try
			{
				NoTypeSignal noTypeSignal = new NoTypeSignal();
				noTypeSignal.fillShortData(strs);
				imsi = noTypeSignal.getImsi();
				// long imsi, int time, int timeSpan, int dataType
				ImsiTimeKey imsitimeKey = new ImsiTimeKey(Long.parseLong(imsi), noTypeSignal.stime,
						noTypeSignal.stime/600*600, 0);

				context.write(imsitimeKey, value);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				// no handle Exception
			}

			// context.write(ImsiTimeKey,value)
		}
	}

}
