

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import StructData.StaticConfig;

public class TestMR
{
	public static class MroMapper extends Mapper<Object, Text, Text, Text>
	{  
	   private String eci = "";
	   private String beginTime = "";
	   private Text keyText = new Text();
	  	   
	   public void map(Object key, Text value, Context context) 
			   throws IOException, InterruptedException 
	   {		   
		  String[] valstrs = value.toString().split("\t", -1);
		  
		  if(valstrs.length < 148)
		  {
			  return;
		  }
		  
		  beginTime = valstrs[2];
		  eci = valstrs[11];
		 	  
		  if(eci.length() == 0
		    || beginTime.length() == 0)
		  {
			  return;
		  }
		  
		  if(eci.equals("75704864"))
		  {
			  keyText.set(String.valueOf(StaticConfig.DataType_SIGNAL_MR_All));
			  //keyText.set(objCellId);
			  context.write(keyText, value);
		  }  
	   }    
	}		
	
	public static class MyReducer extends Reducer<Text, Text, NullWritable, Text>
	{	
        private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<NullWritable,Text> mos;
        private String outpath_mro;
        private String outpath_xdr;

        public void setup(Context context) 
        {
        	Configuration conf = context.getConfiguration();
        	
        	outpath_mro = conf.get("mastercom.mroxdrmerge.test.outpath_mro");
        	outpath_xdr = conf.get("mastercom.mroxdrmerge.test.outpath_xdr");
        	
        	mos = new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<NullWritable, Text>(context);
        }
	    
		public void reduce(Text key,  Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException
		{
			for(Text value : values)
			{
			    int dataType = Integer.valueOf(key.toString());
			    if(dataType == StaticConfig.DataType_SIGNAL_MR_All)
			    {
			    	mos.write("mro", NullWritable.get(), value, makeFileName(outpath_mro, "mro"));  
			    	//mos.write("mro", NullWritable.get(), value, "/mytest/mroxdrmerge/input_mro3/"); 
			    }	
			    else if(dataType == StaticConfig.DataType_SIGNAL_XDR)
			    {
			    	mos.write("xdr", NullWritable.get(), value, makeFileName(outpath_xdr, "xdr"));  
			    	//mos.write("xdr", NullWritable.get(), value, "/mytest/mroxdrmerge/input_xdr3/"); 
			    }
			
			}
			
			/*
			Text result = new Text();
			result.set(key.toString() + "_" + count);
			mos.write("mro", NullWritable.get(), result , makeFileName(outpath_mro, "mro"));
			*/

		}
		
		private String makeFileName(String path, String name)
		{
			return path + name;
		}
		
		
	   @Override
	   protected void cleanup(Context context) throws IOException, InterruptedException 
	   {
		   mos.close();
       }
		
	}
	
	public static void main(String[] args) throws Exception 
	{ 	
		Configuration conf = new Configuration();
		
		System.out.println(Math.abs(String.valueOf("154660097").hashCode()) % 432);

	    if (args.length != 4) 
		{
		    System.err.println("Usage: MroXdrMerger TestData <in-mro> <in-xdr> <out-mro> <out-xdr>");
		    System.exit(2);
	    }
	    
	    String inpath_mro = args[0];
	    String inpath_xdr = args[1];
	    String outpath_mro = args[2];
	    String outpath_xdr = args[3];
	    
	    conf.set("mastercom.mroxdrmerge.test.outpath_mro", outpath_mro);
	    conf.set("mastercom.mroxdrmerge.test.outpath_xdr", outpath_xdr);
           
        Job job = Job.getInstance(conf,"MroXdrMergeTest");      
        job.setNumReduceTasks(40);	
                
        job.setJarByClass(Main.class);  
        job.setReducerClass(MyReducer.class);           
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class); 
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
              
	    MultipleInputs.addInputPath(job, new Path(inpath_mro), TextInputFormat.class, MroMapper.class);
   
	    //mapreduce.multipleoutputs
	    MultipleOutputs.addNamedOutput(job, "mro", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "xdr", TextOutputFormat.class, NullWritable.class, Text.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(outpath_mro));
	        
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}
