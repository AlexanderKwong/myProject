package localsimu.cellgrid_merge;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import StructData.Stat_CellGrid_4G;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;

public class MergeCellGridReduce
{
	public static class CellGridMergeReduce extends DataDealReducer<CellGridTimeKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private String cellGridMergPath;
		protected static final Log LOG = LogFactory.getLog(CellGridMergeReduce.class);
		private int limitSampleNum;
		private long rsrpsum;// rsrp总值
		private int rsrptotal; // sample个数

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			limitSampleNum = Integer.parseInt(MainModel.GetInstance().getAppConfig().getLimitSampleNum());
			MainModel.GetInstance().setConf(conf);
			cellGridMergPath = conf.get("mastercom.cellgridmerge.cellGridMergPath");
			this.context = context;
			// 初始化输出控制
			mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.SetOutputPath("mergeCellGrid", cellGridMergPath);
			mosMng.init();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			mosMng.close();
		}

		@Override
		protected void reduce(CellGridTimeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			Stat_CellGrid_4G cellgrid = null;
			CellGridStruct cellgridRecord = null;
			LOG.error("------------------------------");
			for (Text value : values)
			{
				LOG.error(value.toString());
				if (rsrptotal >= limitSampleNum)
				{
					cellgridRecord = new CellGridStruct(key.getItllongitude(), key.getItllatitude(), key.getiCi(),
							rsrpsum / (double) rsrptotal);
					curText.set(cellgridRecord.toString());
					mosMng.write("mergeCellGrid", NullWritable.get(), curText);
					return;
				}
				String temp[] = value.toString().split(",|\t", -1);
				try
				{
					cellgrid = Stat_CellGrid_4G.FillData(temp, 0);
					rsrpsum += cellgrid.tStat.RSRP_nSum;
					rsrptotal += cellgrid.tStat.RSRP_nTotal;
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, value.toString() + "__" + e.getMessage());
					continue;
				}
			}
			// 统计完cellgrid，sample没有超过极限值
			cellgridRecord = new CellGridStruct(key.getItllongitude(), key.getItllatitude(), key.getiCi(),
					rsrpsum / (double) rsrptotal);
			curText.set(cellgridRecord.toString());
			mosMng.write("mergeCellGrid", NullWritable.get(), curText);
		}
	}
}
