package xdr.rail.subway;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import jan.com.hadoop.mapred.DataDealMapper;
import jan.com.hadoop.mapred.MultiOutputMng;
import mroxdrmerge.MainModel;

public class FilterSubwayMapper 
{
	
	public static class mmeMaper extends DataDealMapper<Object, Text, IntWritable, Text>{
		
		private Text curText;
		private IntWritable curInt;

		@Override
		protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			super.setup(context);
			curText = new Text();
			curInt = new IntWritable();
			
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] split = value.toString().split("\\|");
				int eci = Integer.parseInt(split[9]);
				boolean containEci = SubWayCellReader.GetInstance().getEciMap().containsKey(eci);
				String Procedure_Start_Time = split[5];
				String Procedure_End_Time = split[6];

				StringBuffer sb = new StringBuffer(); 
				sb.append(eci);sb.append("\t");
				sb.append(Procedure_Start_Time);sb.append("\t");
				sb.append(Procedure_End_Time);
				curText.set(sb.toString());
				curInt.set(eci);
				if(containEci){
					context.write(curInt,curText);
				}
			}catch(Exception e){
				//TODO 删除
				e.printStackTrace();
			}
			
		}
	}
}


//String value1;
//if (split[5].length() > 13)
//	value1 = split[5].substring(0, 13);
//Date data = new Date(Long.parseLong(value1));