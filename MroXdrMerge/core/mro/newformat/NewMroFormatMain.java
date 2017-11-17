package mro.newformat;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import StructData.StaticConfig;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mro.newformat.MreFormatMapper.MreMapper;
import mro.newformat.MroFormatMapper.MroMapper;
import mro.newformat.MroFormatReducer.StatReducer;
import mroxdrmerge.MainModel;
import util.HdfsHelper;




public class NewMroFormatMain 
{
	protected static final Log LOG = LogFactory.getLog(NewMroFormatMain.class);
    
	private static int reduceNum;
	private static String queueName;
	private static String outpath_date;
	private static String inpath_mro;
	private static String inpath_mre;
	private static String outpath_table;
	private static String outpath;
	private static String path_mroformat;

	
	private static void makeConfig(Configuration conf, String[] args)
	{
	    reduceNum = Integer.parseInt(args[0]);
	    queueName = args[1];
	    outpath_date = args[2];
	    inpath_mro = args[3];
	    inpath_mre = args[4];
	    outpath_table = args[5];
	    
	    for(int i=0; i<args.length; ++i)
	    {
	    	LOG.info(i + ": " + args[i] + "\n");
	    }
	    
	    //table output path
	    outpath = outpath_table + "/output";
	    path_mroformat = outpath_table + "/mroformat_" + outpath_date;
	       
	    LOG.info(outpath); 
	    LOG.info(path_mroformat);   
		
	    
	    if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
	

	    conf.set("mastercom.mroxdrmerge.mroformat.path_mroformat", path_mroformat);
	    
	    //hadoop system set 
	    conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");//default 0.05
	    conf.set("mapreduce.task.io.sort.mb", "1024");
	    conf.set("mapreduce.map.memory.mb", "3072");
	    conf.set("mapreduce.reduce.memory.mb", "8192");
	    conf.set("mapreduce.map.java.opts", "-Xmx2048M");
	    conf.set("mapreduce.reduce.java.opts", "-Xmx6140M");
	    conf.set("mapreduce.reduce.shuffle.memory.limit.percent","0.1");
	    
		//The minimum size chunk that map input should be split into. Note that some file formats may have minimum split sizes that take priority over this setting.
		//long splitMinSize = 512 * 1024 * 1024;
	    //conf.set("mapreduce.input.fileinputformat.split.minsize",String.valueOf(splitMinSize));     
	    
	    //增加压缩
		//Should the outputs of the maps be compressed before being sent across the network. Uses SequenceFile compression.
		//conf.set("mapreduce.map.output.compress","TRUE");
		//If the map outputs are compressed, how should they be compressed?
		//conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.Lz4Codec");
	    
		//Java opts for the task processes. The following symbol, if present, will be interpolated: @taskid@ is replaced by current TaskID. Any other occurrences of '@' will go unchanged. For example, to enable verbose gc logging to a file named for the taskid in /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of: -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc Usage of -Djava.library.path can cause programs to no longer function if hadoop native libraries are used. These values should instead be set as part of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and mapreduce.reduce.env config settings.
		//conf.set("mapred.child.java.opts","-verbose:gc -XX:+PriintGCDetails");
	    
	    //初始化自己的配置管理
	    DataDealConfiguration.create(outpath_table, conf);
	}	
	
	public static Job CreateJob(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();   
	    if (args.length != 6) 
		{
		    System.err.println("Usage: MroFormat <reduce num> <queen type> <date time> <mro path> <mre path>  <result path>");
		    System.err.println("Now error input num is : " + args.length);
		    throw (new Exception("NewMroFormatMain args input error!"));
	    }	    
	    makeConfig(conf, args);
	    
	    //xsh
	    HDFSOper hdfsOper = new HDFSOper(conf);
//	    HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//                                         MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
			
        Job job = Job.getInstance(conf,"MroXdrMerge.mroformat");
        job.setNumReduceTasks(reduceNum);	 
        
        job.setJarByClass(NewMroFormatMain.class);  
        job.setReducerClass(StatReducer.class);         
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);  
         
        String[] inpaths = inpath_mro.split(",",-1);
        for(String ip : inpaths)
        {
        	if(ip.trim().length() > 0)
        	{
        		MultipleInputs.addInputPath(job, new Path(ip), TextInputFormat.class, MroMapper.class); 
        	}	
        }  
        
        inpaths = inpath_mre.split(",",-1);
        for(String ip : inpaths)
        {
        	if(ip.trim().length() == 0 || ip.trim().equals(StaticConfig.InputPath_NoData))
        	{
        		 continue;
        	}
        	
        	if(!hdfsOper.checkFileExist(ip))
        	{
        		System.err.println("Mre path is not exists : " + ip);
        		continue;
        	}
        	MultipleInputs.addInputPath(job, new Path(ip), TextInputFormat.class, MreMapper.class);
        }  
             
	    MultipleOutputs.addNamedOutput(job, "mroformat", TextOutputFormat.class, NullWritable.class, Text.class);
   
	    FileOutputFormat.setOutputPath(job, new Path(outpath));
	    	    
		//检测输出目录是否存在，存在就改名
	    String tarPath = "";
	    HdfsHelper.reNameExistsPath(hdfsOper, outpath_table, tarPath);
	      
	    return job;
	}
	
	public static void main(String[] args) throws Exception 
	{ 			
		Job job = CreateJob(args);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	  
    }

	
	

	
	

}
