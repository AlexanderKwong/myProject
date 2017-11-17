package imsiCellTime.Merge;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;

public class ImsiCellTimeReduce
{
	public static class ImsiCellTimeReducer extends DataDealReducer<ImeiCellTimesKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private String mergedOutPutPath;
		private Context context;
		private int dayNum;
		private long filterTimes;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			mergedOutPutPath = conf.get("mastercom.mroxdrmerge.mergedOutPutPath");
			dayNum = Integer.parseInt(conf.get("mastercom.dayNum"));
			filterTimes = Long.parseLong(conf.get("mastercom.filterTimes"));
			this.context = context;
			if (!mergedOutPutPath.contains(":"))
			{
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			}
			else
			{
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			}
			mosMng.SetOutputPath("ImsiCellTimes", mergedOutPutPath);
			mosMng.init();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
			mosMng.close();
		}

		@Override
		protected void reduce(ImeiCellTimesKey key, Iterable<Text> timesList, Context context) throws IOException, InterruptedException
		{
			long totalTimes = 0;
			long averTimes = 0;
			String outString = "";
			String vals[];
			int xdrnum = 0;
			long longtitude = 0;
			long latitude = 0;
			long averlongtitude = 0;
			long averlatitude = 0;
			String msisdn = "";
			for (Text temp : timesList)
			{
				vals = temp.toString().split(",", -1);
				totalTimes += Long.parseLong(vals[3]);
				long templongtitude = Long.parseLong(vals[4]);
				long templatitude = Long.parseLong(vals[5]);
				msisdn = vals[6];
				if (templongtitude > 0)
				{
					longtitude += templongtitude;
					latitude += templatitude;
					xdrnum++;
				}
			}
			averTimes = totalTimes / dayNum;
			if (xdrnum == 0)
			{
				averlongtitude = 0;
				averlatitude = 0;
			}
			else
			{
				averlongtitude = longtitude / xdrnum;
				averlatitude = latitude / xdrnum;
			}
			if (averTimes > filterTimes)
			{
				int amOrPm = -1;
				if (key.dayhour >= 7 && key.dayhour <= 17)
				{
					amOrPm = 0;
				}
				else if ((key.dayhour >= 18 && key.dayhour <= 23) || (key.dayhour >= 0 && key.dayhour <= 6))
				{
					amOrPm = 1;
				}
				else
				{
					amOrPm = 2;
				}
				outString = key.toString() + "," + averTimes + "," + averlongtitude + "," + averlatitude + "," + msisdn + "," + amOrPm;
				curText.set(outString);
				mosMng.write("ImsiCellTimes", NullWritable.get(), curText);
			}
		}
	}
}
