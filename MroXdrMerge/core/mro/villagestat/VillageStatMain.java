package mro.villagestat;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mro.villagestat.VillageStatMapper.GridPartitioner;
import mro.villagestat.VillageStatMapper.GridSortKeyComparator;
import mro.villagestat.VillageStatMapper.GridSortKeyGroupComparator;
import mro.villagestat.VillageStatMapper.MroDataMapper;
import mro.villagestat.VillageStatMapper.VillageGridMapper;
import mro.villagestat.VillageStatReducer.MroDataFileReducer;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;




public class VillageStatMain 
{
	protected static final Log LOG = LogFactory.getLog(VillageStatMain.class);
	
	private static int reduceNum;
	public static String queueName;
	
	public static String inpath1;
	public static String inpath2;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String path_sample;
	public static String path_grid;
	
	private static void makeConfig(Configuration conf, String[] args)
	{
  
	    reduceNum = Integer.parseInt(args[0]);
	    queueName = args[1];
	    outpath_date = args[2];
	    inpath1 = args[3];//village配置
	    inpath2 = args[4];//mro数据
	    outpath_table = args[5];	    
        	
	    for(int i=0; i<args.length; ++i)
	    {
	    	LOG.info(i + ": " + args[i] + "\n");
	    }
	    
	    //table output path
	    outpath = outpath_table + "/output";
	    path_sample = outpath_table + "/TB_SIGNAL_VILLAGE_SAMPLE_" + outpath_date;
	    path_grid = outpath_table + "/TB_SIGNAL_VILLAGE_GRID_" + outpath_date;
	    
	    
	    LOG.info(path_sample);
	    LOG.info(path_grid); 
	    
	    if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
        //conf.set("hbase.zookeeper.quorum", "master,node001,node002");  
        //conf.set("hbase.zookeeper.property.clientPort","2181");    
	    //conf.set("mapreduce.map.output.compress", "true");
	    //conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec"); 
	    	    
	    conf.set("mastercom.mroxdrmerge.mro.villagestat.path_sample", path_sample);
	    conf.set("mastercom.mroxdrmerge.mro.villagestat.path_grid", path_grid);
	    
	    //hadoop system set 
	    conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");//default 0.05
	    conf.set("mapreduce.task.io.sort.mb", "1024");
	    conf.set("mapreduce.map.memory.mb", "3072");
	    conf.set("mapreduce.reduce.memory.mb", "8192");
	    conf.set("mapreduce.map.java.opts", "-Xmx2048M");
	    conf.set("mapreduce.reduce.java.opts", "-Xmx6140M");
	    
		//The minimum size chunk that map input should be split into. Note that some file formats may have minimum split sizes that take priority over this setting. 
	    //将小文件进行整合
		long splitMinSize = 512 * 1024 * 1024;
	    conf.set("mapreduce.input.fileinputformat.split.maxsize",String.valueOf(splitMinSize));   
		long minsizePerNode = 10 * 1024 * 1024;
	    conf.set("mapreduce.input.fileinputformat.split.minsize.per.node",String.valueOf(minsizePerNode));
		long minsizePerRack = 32 * 1024 * 1024;
	    conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack",String.valueOf(minsizePerRack));
	    
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
		    //中间过程压缩
		    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec"); 
		    conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=/usr/local/hadoop/lzo/lib"); 
		    conf.set("mapreduce.map.output.compress", "true"); 
		    conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		}
	    
	    //初始化自己的配置管理
	    DataDealConfiguration.create(outpath_table, conf);
	}	
	
	public static Job CreateJob(String[] args) throws Exception 
	{		
		Configuration conf = new Configuration();   
        
	    if (args.length != 6) 
		{
		    System.err.println("Usage: MroFormat <in-mro> <in-xdr> <sample tbname> <event tbname>");
		    throw (new Exception("VillageStatMain args input error!"));
	    }
	    makeConfig(conf, args);
	    
		//检测输出目录是否存在，存在就改名
	    //xsh
	    HDFSOper hdfsOper = new HDFSOper(conf);
//	    HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//                                         MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());   
	    
        Job job = Job.getInstance(conf,"MroXdrMerge.mro.villagestat"  + ":" + outpath_date);
        job.setNumReduceTasks(reduceNum);	 
        
        job.setJarByClass(VillageStatMain.class);
        job.setReducerClass(MroDataFileReducer.class); 
        job.setSortComparatorClass(GridSortKeyComparator.class);
        job.setPartitionerClass(GridPartitioner.class);
        job.setGroupingComparatorClass(GridSortKeyGroupComparator.class);           
        job.setMapOutputKeyClass(GridTypeKey.class);
        job.setMapOutputValueClass(Text.class);  
        
        //set reduce num
        long inputSize = 0;
        int reduceNum = 1;
        
        String[] inpaths = inpath1.split(",", -1);
        for(String tm_inpath_xdr : inpaths)
        {
        	inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false);
        }  
        
        inpaths = inpath2.split(",", -1);
        for(String tm_inpath_xdr : inpaths)
        {
        	inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false);
        } 
        
        
        if(inputSize > 0)
        {
        	double sizeG = inputSize * 1.0 / (1024*1024*1024);
        	int sizePerReduce = 5;
        	reduceNum = Math.max((int)(sizeG / sizePerReduce), reduceNum);
        		
        	LOG.info("total input size of data is : " + sizeG + " G ");
        	LOG.info("the count of reduce to go is " + reduceNum);
        }
        
        job.setNumReduceTasks(reduceNum);   
        ///////////////////////////////////////////////////////
        
        if(hdfsOper.checkFileExist(inpath1))
        {
            MultipleInputs.addInputPath(job, new Path(inpath1), CombineTextInputFormat.class, VillageGridMapper.class);  
        }
        MultipleInputs.addInputPath(job, new Path(inpath2), CombineTextInputFormat.class, MroDataMapper.class); 
        
        //job.setOutputFormatClass(MultiTableOutputFormat.class); 
        
	    MultipleOutputs.addNamedOutput(job, "mrosample", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "mrogrid", TextOutputFormat.class, NullWritable.class, Text.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(outpath));
	     

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
