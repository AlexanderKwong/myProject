
import java.io.IOException;
import java.util.Random;

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


public class XDR_YD
{

	public static class XdrYDMapper extends Mapper<Object, Text, NullWritable, Text>
	{
		private Text keyText = new Text();
		private Text valueText = new Text();
		private StringBuilder tmSb = new StringBuilder();
		private String strutf;
		int pos;
		String baseStr = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789";  
		private Random rr = new Random();
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			strutf = new String(value.toString().getBytes(), "UTF-8");

			String[] valstrs = strutf.toString().split("\t", -1);

			if (valstrs.length != 36)
			{
				return;
			}

			if (valstrs[22].equals("5") && valstrs[23].equals("200"))
			{
				valueText.set(value.toString() + "\t" + getStr());
			}
			else 
			{
				valueText.set(value.toString() + "\t");
			}

			context.write(NullWritable.get(), valueText);
		}

		public char getChar()
		{	
			int flag = (int)(System.currentTimeMillis() - System.currentTimeMillis() /100000*100000);
			int random = (int) Math.abs((Math.random() * flag) % (baseStr.length()));
			return baseStr.charAt(random);
		}

		public String getStr()
		{
			int flag = (int)(System.currentTimeMillis() - System.currentTimeMillis() /100000*100000);
			int random = (int) (Math.random() * flag) % 1024;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < random; ++i)
			{
				sb.append(getChar());
			}
			return sb.toString();
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

				valText.set(keys[0] + "_" + keys[1] + vals[0] + "\t" + vals[1] + "_" + vals[2] + "_" + vals[3] + "_"
						+ vals[4]);
				context.write(NullWritable.get(), valText);
			}

		}

	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
	    String inpath_xdr = args[0];
	    String outpath_xdr = args[1];
	    String quene = args[2];

//		String inpath_xdr = "/seq/S1U_CUT/20160309/";
//		String outpath_xdr = "/seq/S1U_CUT/20160309_NEW/";

		conf.set("mapreduce.job.queuename", quene);

		Job job = Job.getInstance(conf, "XDR_YD");

		job.setJarByClass(XDR_YD.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(XdrYDMapper.class);
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
