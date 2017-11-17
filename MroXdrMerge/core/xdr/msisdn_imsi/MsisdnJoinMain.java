package xdr.msisdn_imsi;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealConfiguration;
import mroxdrmerge.MainModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import xdr.msisdn_imsi.MsisdnJoinMapper.MsisdnMapper;
import xdr.msisdn_imsi.MsisdnJoinMapper.MsisdnPartitioner;
import xdr.msisdn_imsi.MsisdnJoinMapper.MsisdnSortKeyComparator;
import xdr.msisdn_imsi.MsisdnJoinMapper.MsisdnSortKeyGroupComparator;
import xdr.msisdn_imsi.MsisdnJoinMapper.UserInfoMapper;
import xdr.msisdn_imsi.MsisdnJoinReducer.MsisdnReducer;

public class MsisdnJoinMain
{

	private static String outpath_date;
	// protected static final Log LOG = LogFactory.getLog(MsisdnJoinMain.class);

	private static String userInfoPath;
	private static String dayHour;
	private static String outputPath;
	private static String outpath_table;

	public static void main(String[] args)
	{
		try
		{
			userInfoPath = args[0];
			dayHour = args[1];
			outputPath = args[2];
			outpath_table = args[3];


			Configuration conf = new Configuration();
			MainModel.GetInstance().setConf(conf);
			 conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
			// 将小文件进行整合
			long splitMinSize = 128 * 1024 * 1024;
			conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(splitMinSize));
			long minsizePerNode = 10 * 1024 * 1024;
			conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(minsizePerNode));
			long minsizePerRack = 32 * 1024 * 1024;
			conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(minsizePerRack));
			// 初始化自己的配置管理

			DataDealConfiguration.create(outpath_table, conf);
			conf.set("mapreduce.job.queuename", "root.bdoc.renter_1.renter_9.dev_571");
			conf.set("hadoop.security.bdoc.access.id", "80ca02329e5bdfd0bc79");
			conf.set("hadoop.security.bdoc.access.key", "facd3668468f2b4bb3e4064b9d88ea414ac3acfc");

			String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
			Job job = Job.getInstance(conf, "XDRLocAll" + ":" + outpath_date);
			job.setJarByClass(MsisdnJoinMain.class);
			job.setReducerClass(MsisdnReducer.class);

			job.setMapOutputKeyClass(MsisdnKey.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setSortComparatorClass(MsisdnSortKeyComparator.class);

			job.setPartitionerClass(MsisdnPartitioner.class);

			job.setGroupingComparatorClass(MsisdnSortKeyGroupComparator.class);

			long inputDirSize = 0;
			int reduceNum = 1;
			HDFSOper hdfsOper = null;
			hdfsOper = new HDFSOper(conf);
			
			if(hdfsOper.checkDirExist("/user/wangjun/O_DPI_VL_RTP/load_time_d="+dayHour)){
				
				inputDirSize = hdfsOper.getHdfs().getContentSummary(new Path("/user/wangjun/O_DPI_VL_RdfTP/load_time_d="+dayHour)).getLength();
			}
			
			if (hdfsOper.checkDirExist(userInfoPath))
			{
				inputDirSize += hdfsOper.getSizeOfPath(userInfoPath, false);
			}

			job.setNumReduceTasks(reduceNum);

			MultipleInputs.addInputPath(job, new Path(userInfoPath), CombineTextInputFormat.class,
					UserInfoMapper.class);

			// /user/wangjun/O_DPI_VL_RTP/load_time_d=20170830/load_time_h=10/load_time_m=00
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			String[] minuts = new String[] { "00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55" };
			
			for (int j = 0; j < hours.length; j++)
			{
				for (int i = 0; i < minuts.length; i++)
				{
					String rtpDataPath = "/user/wangjun/O_DPI_VL_RTP/load_time_d="+dayHour+"/load_time_h=" + hours[j]
							+ "/load_time_m="+minuts[i];
					if (hdfsOper.checkDirExist(rtpDataPath))
					{
						MultipleInputs.addInputPath(job, new Path(rtpDataPath), CombineTextInputFormat.class,
								MsisdnMapper.class);
					}
				}

			}
			if (inputDirSize > 0)
			{
				double sizeG = inputDirSize * 1.0 / (1024 * 1024 * 1024);
				int sizePerReduce = 2;
				reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);
			}
			
			

			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			if (!job.waitForCompletion(true))
			{
				System.out.println("MsisdnJoin Job error! stop run.");
				throw (new Exception("system.exit1"));
			}
		}
		catch (Exception e)
		{ 
			e.printStackTrace();
		}

	}

}