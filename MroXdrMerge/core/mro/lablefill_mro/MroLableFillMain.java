package mro.lablefill_mro;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
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
import mro.lablefill.CellTimeKey;
import mro.lablefill.MroLableFileReducer.MroDataFileReducer;
import mro.lablefill.MroLableMapper.CellPartitioner;
import mro.lablefill.MroLableMapper.CellSortKeyComparator;
import mro.lablefill.MroLableMapper.CellSortKeyGroupComparator;
import mro.lablefill.MroLableMapper.MroDataMapper;
import mro.lablefill.MroLableMapper.XdrLocationMapper;
import mro.lablefill_mro.MroLableFileReducer.MroDataFileReducers;
import mro.lablefill_mro.MroLableMapper.MroDataMappers;
import mro.lablefill_mro.MroLableMapper.XdrLocationMappers;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.HdfsHelper;

public class MroLableFillMain
{
	protected static final Log LOG = LogFactory.getLog(MroLableFillMain.class);

	private static int reduceNum;
	public static String queueName;

	public static String inpath_xdr;
	public static String inpath_mre;
	public static String inpath_mro;
	public static String outpath;
	public static String outpath_table;
	public static String outpath_date;
	public static String path_sample;
	public static String path_event;
	public static String path_cell;
	public static String path_cell_freq;
	public static String path_cellgrid;
	public static String path_grid;
	public static String path_ImsiSampleIndex;
	public static String path_ImsiEventIndex;
	public static String path_locMore;
	public static String path_mroMore;
	public static String path_myLog;

	public static String path_grid_dt;
	public static String path_grid_dt_freq;
	public static String path_grid_cqt;
	public static String path_grid_cqt_freq;
	public static String path_sample_dt;
	public static String path_sample_dtex;
	public static String path_sample_cqt;
	public static String path_sample_index_dt;
	public static String path_sample_index_cqt;

	public static String path_useract_cell;

	private static void makeConfig_home(Configuration conf, String[] args)
	{

		reduceNum = 1000;
		queueName = args[1];
		outpath_date = args[2];
		inpath_xdr = args[3];
		inpath_mro = args[4];
		outpath_table = args[5];
		inpath_mre = args[6];

		for (int i = 0; i < args.length; ++i)
		{
			LOG.info(i + ": " + args[i] + "\n");
		}

		// table output path
		outpath = outpath_table + "/output";
		path_sample = outpath_table + "/TB_SIGNAL_SAMPLE_" + outpath_date;
		path_event = outpath_table + "/TB_SIGNAL_EVENT_" + outpath_date;
		path_cell = outpath_table + "/TB_SIGNAL_CELL_" + outpath_date;
		path_cell_freq = outpath_table + "/TB_FREQ_SIGNAL_CELL_" + outpath_date;
		path_cellgrid = outpath_table + "/TB_SIGNAL_CELLGRID_" + outpath_date;
		path_grid = outpath_table + "/TB_SIGNAL_GRID_" + outpath_date;
		path_ImsiSampleIndex = outpath_table + "/TB_SIGNAL_INDEX_SAMPLE_" + outpath_date;
		path_ImsiEventIndex = outpath_table + "/TB_SIGNAL_INDEX_EVENT_" + outpath_date;
		path_locMore = outpath_table + "/LOC_MORE_" + outpath_date;
		path_mroMore = outpath_table + "/MRO_MORE_" + outpath_date;
		path_myLog = outpath_table + "/MYLOG_" + outpath_date;

		path_grid_dt = outpath_table + "/TB_DTSIGNAL_GRID_" + outpath_date;
		path_grid_dt_freq = outpath_table + "/TB_FREQ_DTSIGNAL_GRID_" + outpath_date;
		path_grid_cqt = outpath_table + "/TB_CQTSIGNAL_GRID_" + outpath_date;
		path_grid_cqt_freq = outpath_table + "/TB_FREQ_CQTSIGNAL_GRID_" + outpath_date;
		path_sample_dt = outpath_table + "/TB_DTSIGNAL_SAMPLE_" + outpath_date;
		path_sample_dtex = outpath_table + "/TB_DTEXSIGNAL_SAMPLE_" + outpath_date;
		path_sample_cqt = outpath_table + "/TB_CQTSIGNAL_SAMPLE_" + outpath_date;
		path_sample_index_dt = outpath_table + "/TB_DTSIGNAL_INDEX_SAMPLE_" + outpath_date;
		path_sample_index_cqt = outpath_table + "/TB_CQTSIGNAL_INDEX_SAMPLE_" + outpath_date;

		path_useract_cell = outpath_table + "/TB_SIG_USER_BEHAVIOR_LOC_MR_" + outpath_date.substring(3);

		LOG.info(path_sample);
		LOG.info(path_event);
		LOG.info(path_cell);
		LOG.info(path_cell_freq);
		LOG.info(path_cellgrid);
		LOG.info(path_grid);
		LOG.info(path_ImsiSampleIndex);
		LOG.info(path_ImsiEventIndex);
		LOG.info(path_locMore);
		LOG.info(path_mroMore);
		LOG.info(path_myLog);

		LOG.info(path_grid_dt);
		LOG.info(path_grid_dt_freq);
		LOG.info(path_grid_cqt);
		LOG.info(path_grid_cqt_freq);
		LOG.info(path_sample_dt);
		LOG.info(path_sample_dtex);
		LOG.info(path_sample_cqt);
		LOG.info(path_sample_index_dt);
		LOG.info(path_sample_index_cqt);

		LOG.info(path_useract_cell);

		if (!queueName.equals("NULL"))
		{
			conf.set("mapreduce.job.queuename", queueName);
		}

		// conf.set("hbase.zookeeper.quorum", "master,node001,node002");
		// conf.set("hbase.zookeeper.property.clientPort","2181");
		// conf.set("mapreduce.map.output.compress", "true");
		// conf.set("mapreduce.map.output.compress.codec",
		// "org.apache.hadoop.io.compress.Lz4Codec");

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample", path_sample);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_event", path_event);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cell", path_cell);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cell_freq", path_cell_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid", path_cellgrid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid", path_grid);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ImsiSampleIndex", path_ImsiSampleIndex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_ImsiEventIndex", path_ImsiEventIndex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_myLog", path_myLog);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_locMore", path_locMore);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_mroMore", path_mroMore);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt", path_grid_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt_freq", path_grid_dt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt", path_grid_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt_freq", path_grid_cqt_freq);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_dt", path_sample_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_dtex", path_sample_dtex);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_cqt", path_sample_cqt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_dt", path_sample_index_dt);
		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_cqt", path_sample_index_cqt);

		conf.set("mastercom.mroxdrmerge.mro.locfillex.path_useract_cell", path_useract_cell);

		// hadoop system set
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");// default
																		// 0.05
		conf.set("mapreduce.task.io.sort.mb", "1024");
		conf.set("mapreduce.map.memory.mb", "3072");
		conf.set("mapreduce.reduce.memory.mb", "8192");
		conf.set("mapreduce.map.java.opts", "-Xmx2048M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx6140M");

		// The minimum size chunk that map input should be split into. Note that
		// some file formats may have minimum split sizes that take priority
		// over this setting.
		long splitMinSize = 512 * 1024 * 1024;
		conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(splitMinSize));

		if (MainModel.GetInstance().getCompile().Assert(CompileMark.LZO_Compress))
		{
			// 中间过程压缩
			conf.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
			conf.set("mapreduce.map.output.compress", "LD_LIBRARY_PATH=/usr/local/hadoop/lzo/lib");
			conf.set("mapreduce.map.output.compress", "true");
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");
		}

		// 初始化自己的配置管理
		DataDealConfiguration.create(outpath_table, conf);
	}

	public static Job CreateJob(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		return CreateJob(conf, args);
	}

	public static Job CreateJob(Configuration conf, String[] args) throws Exception
	{
		if (args.length != 7)
		{
			System.err.println("Usage: Mro_loc <in-mro> <in-xdr> <sample tbname> <event tbname>");
			throw (new Exception("MroLableFillMain args input error!"));
		}
		makeConfig_home(conf, args);

		// 检测输出目录是否存在，存在就改�

		HDFSOper hdfsOper = new HDFSOper(conf);
		// HDFSOper hdfsOper = new
		// HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
		// MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());

		Job job = Job.getInstance(conf, "MroXdrMerge.mro.locfillex" + ":" + outpath_date);
		job.setNumReduceTasks(reduceNum);

		job.setJarByClass(MroLableFillMain.class);
		job.setReducerClass(MroDataFileReducers.class);
		job.setSortComparatorClass(CellSortKeyComparator.class);
		job.setPartitionerClass(CellPartitioner.class);
		job.setGroupingComparatorClass(CellSortKeyGroupComparator.class);
		job.setMapOutputKeyClass(CellTimeKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reduce num
		long inputSize = 0;
		int reduceNum = 1;
		String[] inpaths;

		if (!inpath_xdr.equals("NULL"))
		{
			inpaths = inpath_xdr.split(",", -1);
			for (String tm_inpath_xdr : inpaths)
			{
				inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false);
			}
		}

		inpaths = inpath_mro.split(",", -1);
		for (String tm_inpath_xdr : inpaths)
		{
			inputSize += hdfsOper.getSizeOfPath(tm_inpath_xdr, false);
		}

		inpaths = inpath_mre.split(",", -1);
		for (String tm_inpath_mre : inpaths)
		{
			inputSize += hdfsOper.getSizeOfPath(tm_inpath_mre, false);
		}

		if (inputSize > 0)
		{
			double sizeG = inputSize * 1.0 / (1024 * 1024 * 1024);
			int sizePerReduce = 2;
			reduceNum = Math.max((int) (sizeG / sizePerReduce), reduceNum);

			LOG.info("total input size of data is : " + sizeG + " G ");
			LOG.info("the count of reduce to go is : " + reduceNum);

		}

		job.setNumReduceTasks(reduceNum);

		///////////////////////////////////////////////////////

		if (!inpath_xdr.equals("NULL"))
		{
			MultipleInputs.addInputPath(job, new Path(inpath_xdr), TextInputFormat.class, XdrLocationMappers.class);
		}
		MultipleInputs.addInputPath(job, new Path(inpath_mro), TextInputFormat.class, MroDataMappers.class);
		MultipleInputs.addInputPath(job, new Path(inpath_mre), TextInputFormat.class, MroDataMappers.class);

		MultipleOutputs.addNamedOutput(job, "mrosample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mroevent", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrocell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrocellfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrocellgrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mrogrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsisampleindex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "imsieventindex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "myLog", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "locMore", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mroMore", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "griddt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "griddtfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gridcqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gridcqtfreq", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampledt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampledtex", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "samplecqt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampleindexdt", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sampleindexcqt", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "useractcell", TextOutputFormat.class, NullWritable.class, Text.class);

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
