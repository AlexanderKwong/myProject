package xdr.lablefill.dtcqt;

import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import mro.lablefillex_uemro.FigureFixedOutput;
import mro.lablefillex_uemro.UEMroLableFileReducer.MroDataFileReducers;
import mroxdrmerge.MainModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import StructData.DT_Sample_4G;
import StructData.GridItem;
import StructData.NoTypeSignal;
import xdr.lablefill.LabelDeal;
import xdr.lablefill.TestTypeDeal;
import xdr.lablefill.by23g.ImsiTimeKey;


import cellconfig.CellConfig;
public class XdrLabelDealReducer
{

	public static class XdrDtCqtDataReducer extends DataDealReducer<ImsiTimeKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private static Text curText;
		private String path_sample;
		private String path_event;
		private String path_cell;
		private String path_cell_freq;
		private String path_cellgrid;
		private String path_grid;
		private String path_ImsiSampleIndex;
		private String path_ImsiEventIndex;
		private String path_myLog;
		private String path_locMore;
		private String path_mroMore;
		private String path_grid_dt;
		private String path_grid_dt_freq;
		private String path_grid_cqt;
		private String path_grid_cqt_freq;
		private String path_sample_dt;
		private String path_sample_dtex;
		private String path_sample_cqt;
		private String path_sample_index_dt;
		private String path_sample_index_cqt;
		private String path_useract_cell;
		private String path_ten_grid;
		private String path_ten_grid_dt;
		private String path_ten_grid_dt_freq;
		private String path_ten_grid_cqt;
		private String path_ten_grid_cqt_freq;
		private String path_ten_cellgrid;
		private FigureFixedOutput figureMroFix;
		private Context context;
		protected static final Log LOG = LogFactory.getLog(MroDataFileReducers.class);
		
		@Override
		protected void setup(Reducer<ImsiTimeKey, Text, NullWritable, Text>.Context context) throws IOException,
				InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			figureMroFix = new FigureFixedOutput(context, conf);
			figureMroFix.setup();

			path_sample = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample");
			path_event = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_event");
			path_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell");
			path_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell_freq");
			path_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid");
			path_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid");
			path_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiSampleIndex");
			path_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiEventIndex");
			path_myLog = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_myLog");
			path_locMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locMore");
			path_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mroMore");

			path_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt");
			path_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt_freq");
			path_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt");
			path_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt_freq");
			path_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dt");
			path_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dtex");
			path_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_cqt");
			path_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_dt");
			path_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_cqt");

			path_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_useract_cell");

			path_ten_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid");
			path_ten_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt");
			path_ten_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt_freq");
			path_ten_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt");
			path_ten_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt_freq");
			path_ten_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_cellgrid");

			this.context = context;

			// 初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

			mosMng.SetOutputPath("mrosample", path_sample);
			mosMng.SetOutputPath("mroevent", path_event);
			mosMng.SetOutputPath("mrocell", path_cell);
			mosMng.SetOutputPath("mrocellfreq", path_cell_freq);
			mosMng.SetOutputPath("mrocellgrid", path_cellgrid);
			mosMng.SetOutputPath("mrogrid", path_grid);
			mosMng.SetOutputPath("imsisampleindex", path_ImsiSampleIndex);
			mosMng.SetOutputPath("imsieventindex", path_ImsiEventIndex);
			mosMng.SetOutputPath("myLog", path_myLog);
			mosMng.SetOutputPath("locMore", path_locMore);
			mosMng.SetOutputPath("mroMore", path_mroMore);

			mosMng.SetOutputPath("griddt", path_grid_dt);
			mosMng.SetOutputPath("griddtfreq", path_grid_dt_freq);
			mosMng.SetOutputPath("gridcqt", path_grid_cqt);
			mosMng.SetOutputPath("gridcqtfreq", path_grid_cqt_freq);
			mosMng.SetOutputPath("sampledt", path_sample_dt);
			mosMng.SetOutputPath("sampledtex", path_sample_dtex);
			mosMng.SetOutputPath("samplecqt", path_sample_cqt);
			mosMng.SetOutputPath("sampleindexdt", path_sample_index_dt);
			mosMng.SetOutputPath("sampleindexcqt", path_sample_index_cqt);

			mosMng.SetOutputPath("useractcell", path_useract_cell);

			mosMng.SetOutputPath("tenmrogrid", path_ten_grid);
			mosMng.SetOutputPath("tengriddt", path_ten_grid_dt);
			mosMng.SetOutputPath("tengriddtfreq", path_ten_grid_dt_freq);
			mosMng.SetOutputPath("tengridcqt", path_ten_grid_cqt);
			mosMng.SetOutputPath("tengridcqtfreq", path_ten_grid_cqt_freq);
			mosMng.SetOutputPath("tenmrocellgrid", path_ten_cellgrid);

			mosMng.init();
			////////////////////
			
			// 初始化小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
				throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
			}

			// 打印状态日志
			LOGHelper.GetLogger().writeLog(LogType.info,
					"cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
		}

		@Override
		protected void cleanup(Reducer<ImsiTimeKey, Text, NullWritable, Text>.Context context) throws IOException,
				InterruptedException
		{
			
			try{
				super.cleanup(context);
				figureMroFix.cleanup();
			}

			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
			}
		}

		public void reduce(ImsiTimeKey key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			// 1. 调用label deal 进行dtcqt判断，调用testType进行dtcqt回填
 
			LabelDeal labelDeal = new LabelDeal(key.getImsi(), mosMng);

			Iterator<Text> it = values.iterator();

			ArrayList<NoTypeSignal> noTypeSignalList = new ArrayList<>();
			while (it.hasNext())
			{
				String next = it.next().toString();
				String[] split = next.split("\t");
				NoTypeSignal noTypeSignal = new NoTypeSignal();
				noTypeSignal.fillData(split);
				noTypeSignalList.add(noTypeSignal);
			} 
			// 计算XDR数据的用户运动标签
			labelDeal.deal(noTypeSignalList);
			labelDeal.finalDeal();
			// 定性用户DT、CQT 
			TestTypeDeal testTypeDeal = new TestTypeDeal(key.getImsi(), labelDeal.IsDDDriver(),
					labelDeal.getUserHomeCellMap());
			testTypeDeal.deal(noTypeSignalList);
			
			for (int i = 0; i < noTypeSignalList.size(); i++)
			{
				noTypeSignalList.get(i).fadbackFill();
			}
			// 接下来直接统计
			for (int i = 0; i < noTypeSignalList.size(); i++)
			{
				DT_Sample_4G dt_sample_4G = noTypeSignalList.get(i).dt_sample_4G;
//				// 开始处理
				figureMroFix.statMro(dt_sample_4G);
				figureMroFix.statKpi(dt_sample_4G);
			}

		}
	}

}
