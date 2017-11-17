package xdr.lablefill.by23g;



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

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mroxdrmerge.MainModel;
import util.HdfsHelper;
import xdr.lablefill.by23g.XdrLableFileReducer.XdrDataFileReducer;
import xdr.lablefill.by23g.XdrLableMapper.ImsiPartitioner;
import xdr.lablefill.by23g.XdrLableMapper.ImsiSortKeyComparator;
import xdr.lablefill.by23g.XdrLableMapper.ImsiSortKeyGroupComparator;
import xdr.lablefill.by23g.XdrLableMapper.XdrDataMapper;


public class XdrLableFill23GMain 
{
	protected static final Log LOG = LogFactory.getLog(XdrLableFill23GMain.class);
	
	private static int reduceNum;
	public static String queueName;
	
	//public static String inpath_xdr;
	public static String inpath_xdr_23g;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	
	public static String path_event_dt_2g;
	public static String path_event_dt_3g;
	public static String path_event_dtex_2g;
	public static String path_event_dtex_3g;
	public static String path_event_cqt_2g;
	public static String path_event_cqt_3g;
	
	public static String path_event_err_23g;
	
	public static String path_grid_2g;
	public static String path_grid_3g;
	public static String path_grid_dt_2g;
	public static String path_grid_dt_3g;
	public static String path_grid_cqt_2g;
	public static String path_grid_cqt_3g;
	
	public static String path_cellgrid_2g;
	public static String path_cellgrid_3g;
	public static String path_cellgrid_dt_2g;
	public static String path_cellgrid_dt_3g;
	public static String path_cellgrid_cqt_2g;
	public static String path_cellgrid_cqt_3g;
	
	public static String path_cell_2g;
	public static String path_cell_3g;

	private static void makeConfig(Configuration conf, String[] args)
	{
   
	    reduceNum = Integer.parseInt(args[0]);
	    queueName = args[1];
	    outpath_date = args[2];
	    inpath_xdr_23g = args[3];
	    outpath_table = args[4];
	    
	    for(int i=0; i<args.length; ++i)
	    {
	    	LOG.info(i + ": " + args[i] + "\n");
	    }
	    
	    //table output path
	    outpath = outpath_table + "/output";
	    path_event_dt_2g = outpath_table + "/TB_2G_DTSIGNAL_EVENT_" + outpath_date;
	    path_event_dt_3g = outpath_table + "/TB_3G_DTSIGNAL_EVENT_" + outpath_date;
	    path_event_dtex_2g = outpath_table + "/TB_2G_DTEXSIGNAL_EVENT_" + outpath_date;
	    path_event_dtex_3g = outpath_table + "/TB_3G_DTEXSIGNAL_EVENT_" + outpath_date;
	    path_event_cqt_2g = outpath_table + "/TB_2G_CQTSIGNAL_EVENT_" + outpath_date;
	    path_event_cqt_3g = outpath_table + "/TB_3G_CQTSIGNAL_EVENT_" + outpath_date;	    
	    
	    path_event_err_23g = outpath_table + "/TB_23G_ERRSIGNAL_EVENT_" + outpath_date;
	    
	    path_grid_2g = outpath_table + "/TB_2G_SIGNAL_GRID_" + outpath_date;
	    path_grid_3g = outpath_table + "/TB_3G_SIGNAL_GRID_" + outpath_date;
	    path_grid_dt_2g = outpath_table + "/TB_2G_DTSIGNAL_GRID_" + outpath_date;
	    path_grid_dt_3g = outpath_table + "/TB_3G_DTSIGNAL_GRID_" + outpath_date;
	    path_grid_cqt_2g = outpath_table + "/TB_2G_CQTSIGNAL_GRID_" + outpath_date;
	    path_grid_cqt_3g = outpath_table + "/TB_3G_CQTSIGNAL_GRID_" + outpath_date;
	    
	    path_cellgrid_2g = outpath_table + "/TB_2G_SIGNAL_CELLGRID_" + outpath_date;
	    path_cellgrid_3g = outpath_table + "/TB_3G_SIGNAL_CELLGRID_" + outpath_date;
	    path_cellgrid_dt_2g = outpath_table + "/TB_2G_DTSIGNAL_CELLGRID_" + outpath_date;
	    path_cellgrid_dt_3g = outpath_table + "/TB_3G_DTSIGNAL_CELLGRID_" + outpath_date;
	    path_cellgrid_cqt_2g = outpath_table + "/TB_2G_CQTSIGNAL_CELLGRID_" + outpath_date;
	    path_cellgrid_cqt_3g = outpath_table + "/TB_3G_CQTSIGNAL_CELLGRID_" + outpath_date;
	    
	    path_cell_2g = outpath_table + "/TB_2G_SIGNAL_CELL_" + outpath_date;
	    path_cell_3g = outpath_table + "/TB_3G_SIGNAL_CELL_" + outpath_date;
	    
	    LOG.info(path_event_dt_2g);
	    LOG.info(path_event_dt_3g);
	    LOG.info(path_event_dtex_2g);
	    LOG.info(path_event_dtex_3g);
	    LOG.info(path_event_cqt_2g);
	    LOG.info(path_event_cqt_3g);
	    LOG.info(path_event_err_23g);
	    
	    LOG.info(path_grid_2g);
	    LOG.info(path_grid_3g);
	    LOG.info(path_grid_dt_2g);
	    LOG.info(path_grid_dt_3g);
	    LOG.info(path_grid_cqt_2g);
	    LOG.info(path_grid_cqt_3g);
	    
	    LOG.info(path_cellgrid_2g);
	    LOG.info(path_cellgrid_3g);
	    LOG.info(path_cellgrid_dt_2g);
	    LOG.info(path_cellgrid_dt_3g);
	    LOG.info(path_cellgrid_cqt_2g);
	    LOG.info(path_cellgrid_cqt_3g);
	    
	    LOG.info(path_cell_2g);
	    LOG.info(path_cell_3g);
	    
	    if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}
	      
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dt_2g", path_event_dt_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dt_3g", path_event_dt_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex_2g", path_event_dtex_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_dtex_3g", path_event_dtex_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt_2g", path_event_cqt_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_cqt_3g", path_event_cqt_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_event_err_23g", path_event_err_23g);
	    
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_2g", path_grid_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_3g", path_grid_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt_2g", path_grid_dt_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_dt_3g", path_grid_dt_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt_2g", path_grid_cqt_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_grid_cqt_3g", path_grid_cqt_3g);
	    
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_2g", path_cellgrid_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_3g", path_cellgrid_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_dt_2g", path_cellgrid_dt_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_dt_3g", path_cellgrid_dt_3g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_cqt_2g", path_cellgrid_cqt_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cellgrid_cqt_3g", path_cellgrid_cqt_3g);
	    
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cell_2g", path_cell_2g);
	    conf.set("mastercom.mroxdrmerge.xdr.locfill.path_cell_3g", path_cell_3g);
	    
	    //hadoop system set 
	    conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");//default 0.05
	    conf.set("mapreduce.task.io.sort.mb", "1024");
	    conf.set("mapreduce.map.memory.mb", "3072");
	    conf.set("mapreduce.reduce.memory.mb", "8192");
	    conf.set("mapreduce.map.java.opts", "-Xmx2048M");
	    conf.set("mapreduce.reduce.java.opts", "-Xmx6140M");
	    
	    conf.set("mapreduce.reduce.speculative", "false");//停止推测功能
	    
	    //初始化自己的配置管理
	    DataDealConfiguration.create(outpath_table, conf);
	}	
	
	public static Job CreateJob(String[] args) throws Exception 
	{		
		Configuration conf = new Configuration();   
		//conf = HBaseConfiguration.create(conf);	  
         
	    if (args.length != 5) 
		{
		    System.err.println("Usage: MroFormat <in-mro> <in-xdr> <out path> <out table path> <out path date>");
		    throw (new Exception("XdrLableFill23GMain args input error!"));

	    }
	    makeConfig(conf, args);	        
	    
        Job job = Job.getInstance(conf,"MroXdrMerge.xdr.locfill.23g" + ":" + outpath_date);
        job.setNumReduceTasks(reduceNum);	 
        
        job.setJarByClass(XdrLableFill23GMain.class);  
        job.setReducerClass(XdrDataFileReducer.class);  
        job.setSortComparatorClass(ImsiSortKeyComparator.class);
        job.setPartitionerClass(ImsiPartitioner.class);
        job.setGroupingComparatorClass(ImsiSortKeyGroupComparator.class);    
        job.setMapOutputKeyClass(ImsiTimeKey.class);
        job.setMapOutputValueClass(Text.class);    
        
//        String[] inpaths = inpath_xdr.split(",", -1);
//        for(String tm_inpath_xdr : inpaths)
//        {
//        	MultipleInputs.addInputPath(job, new Path(tm_inpath_xdr), TextInputFormat.class, XdrDataMapper.class); 
//        }  
        MultipleInputs.addInputPath(job, new Path(inpath_xdr_23g), TextInputFormat.class, XdrDataMapper.class); 

	    MultipleOutputs.addNamedOutput(job, "eventdt2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "eventdt3g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "eventdtex2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "eventdtex3g", TextOutputFormat.class, NullWritable.class, Text.class);	    
	    MultipleOutputs.addNamedOutput(job, "eventcqt2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "eventcqt3g", TextOutputFormat.class, NullWritable.class, Text.class);	    
	  
	    MultipleOutputs.addNamedOutput(job, "eventerr23g", TextOutputFormat.class, NullWritable.class, Text.class);
	    
	    MultipleOutputs.addNamedOutput(job, "grid2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "grid3g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "griddt2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "griddt3g", TextOutputFormat.class, NullWritable.class, Text.class);	    
	    MultipleOutputs.addNamedOutput(job, "gridcqt2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "gridcqt3g", TextOutputFormat.class, NullWritable.class, Text.class);	    
    
	    MultipleOutputs.addNamedOutput(job, "cellgrid2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "cellgrid3g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "cellgriddt2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "cellgriddt3g", TextOutputFormat.class, NullWritable.class, Text.class);	    
	    MultipleOutputs.addNamedOutput(job, "cellgridcqt2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "cellgridcqt3g", TextOutputFormat.class, NullWritable.class, Text.class);	    
    	    
	    MultipleOutputs.addNamedOutput(job, "cell2g", TextOutputFormat.class, NullWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "cell3g", TextOutputFormat.class, NullWritable.class, Text.class);	        	 
	    
	    FileOutputFormat.setOutputPath(job, new Path(outpath));
	    
		//检测输出目录是否存在，存在就改名
	    //xsh
	    HDFSOper hdfsOper = new HDFSOper(conf);
//	    HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//                                         MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
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
