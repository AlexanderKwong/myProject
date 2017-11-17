
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XdrHour
{
	
	public static class XdrHourMapper extends Mapper<Object, Text, Text, Text>
	{
		private String beginTime = "";
		private Text keyText = new Text();
		private Text valueText = new Text();
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private Date d_beginTime;
		private String s1apid;
		private String ci;
		private StringBuilder tmSb = new StringBuilder();
		private String imsi = "";
		private String Event_Type = "";
		private String longitude;
		private String latitude;
		private String strutf;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			strutf = new String(value.toString().getBytes(),"UTF-8");;
			String[] valstrs = strutf.toString().split("\t", -1);
			
			if (valstrs[0].toLowerCase().equals("online_id"))
			{
				return;
			}

			if (valstrs.length < 4)
			{
				return;
			}

			beginTime = valstrs[2];
 			s1apid = valstrs[13];
			ci = valstrs[23];

			if (beginTime.length() == 0 || s1apid.length() == 0 || ci.length() == 0)
			{
				return;
			}

			long tmCi = (Long.parseLong(ci) / 100) * 256 + Long.parseLong(ci) % 100;
			ci = String.valueOf(tmCi);

			int stime = 0;
			try
			{
				beginTime = valstrs[2].substring(0, beginTime.length() - 7);
				d_beginTime = format.parse(beginTime);
				stime = (int) (d_beginTime.getTime() / 1000L);
			}
			catch (Exception e)
			{
				return;
			}

			if (d_beginTime.getHours() != 12)
			{
				return;
			}

//			imsi = valstrs[25];
//			Event_Type = valstrs[31];
//			longitude = valstrs[267];
//			latitude = valstrs[268];
//
//			tmSb.delete(0, tmSb.length());
//			tmSb.append(stime);
//			tmSb.append("_");
//			tmSb.append(imsi);
//			tmSb.append("_");
//			tmSb.append(Event_Type);
//			tmSb.append("_");
//			tmSb.append(longitude);
//			tmSb.append("_");
//			tmSb.append(latitude);

			keyText.set(ci + "_" + s1apid + "_" + stime/3600*3600);
			valueText.set(tmSb.toString());
//			context.write(NullWritable.get(), value);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		private Text valText = new Text();

		public void setup(Context context)
		{

		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] vals;
			String[] keys;
			for (Text text : values)
			{
				keys = key.toString().split("_", -1);
				vals = text.toString().split("_", -1);
				
				valText.set(keys[0] + "_" + keys[1] + vals[0] + "\t" + vals[1] + "_" + vals[2] + "_" + vals[3] + "_" + vals[4]);
				context.write(NullWritable.get(), valText);
			}

		}


	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		if (args.length != 3)
		{
			System.err.println("Usage: MroXdrMerger TestData <in-data> <in-data> <hour time> ");
			throw (new Exception("MroXdrMerger args input error!"));
		}

		String inpath_xdr = args[0];
		String outpath_xdr = args[1];
		String hourTime = args[2];

		conf.set("mastercom.mroxdrmerge.XdrHour.inpath_xdr", inpath_xdr);
		conf.set("mastercom.mroxdrmerge.XdrHour.outpath_xdr", outpath_xdr);
		conf.set("mastercom.mroxdrmerge.XdrHour.hourTime", hourTime);
		
		conf.set("mapreduce.job.queuename", "plan");

		Job job = Job.getInstance(conf, "XdrHour");
		
        job.setJarByClass(XdrHour.class);  
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(XdrHourMapper.class);   
        job.setMapOutputKeyClass(NullWritable.class);  
        job.setMapOutputValueClass(Text.class); 
        
        job.setNumReduceTasks(0);	
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);   
        
		FileInputFormat.setInputPaths(job, new Path(inpath_xdr));
		FileOutputFormat.setOutputPath(job, new Path(outpath_xdr));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
