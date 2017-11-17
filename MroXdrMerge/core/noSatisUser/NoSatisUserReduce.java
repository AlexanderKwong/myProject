package noSatisUser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import mro.lablefill.CellTimeKey;
import mro.lablefill.XdrLable;
import mro.lablefill.XdrLableMng;
import mroxdrmerge.MainModel;

public class NoSatisUserReduce
{
	public static class NoSatisUserReducers extends DataDealReducer<CellTimeKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private Context context;
		private String outpath_table;
		private XdrLableMng xdrLableMng;
		private static SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			outpath_table = conf.get("mapreduce.job.oupath");
			String tempData = conf.get("mapreduce.job.date");

			if (outpath_table != null && outpath_table.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

			String filePath = outpath_table + "/Mgos_" + tempData;
			mosMng.SetOutputPath("gmos", filePath);

			filePath = outpath_table + "/WJtdh_" + tempData;
			mosMng.SetOutputPath("wjtdh", filePath);
			this.context = context;
			mosMng.init();
			xdrLableMng = new XdrLableMng();
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			mosMng.close();
		}

		@Override
		public void reduce(CellTimeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{

			if (key.getDataType() == 0)
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "xdr:" + key.toString());
				xdrLableMng = new XdrLableMng();
				for (Text value : values)
				{
					String[] strs = value.toString().split("\t", -1);
					for (int i = 0; i < strs.length; ++i)
					{
						strs[i] = strs[i].trim();
					}

					XdrLable xdrLable;
					try
					{
						xdrLable = XdrLable.FillData(strs, 0);
						xdrLableMng.addXdrLocItem(xdrLable);
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "XdrLable.FillData error ", e);
						continue;
					}
				}
				xdrLableMng.init();
			}
			else
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "gmosWjtdh::" + key.toString());
				Text value;
				String[] strs;
				String outname = "";
				LOGHelper.GetLogger().writeLog(LogType.info, "begin  GLI_MSI, " + xdrLableMng.getSize());
				while (values.iterator().hasNext())
				{
					try
					{
						value = values.iterator().next();
						strs = value.toString().split(",", -1);
						GL_BaseInfo baseInfo = new GL_BaseInfo();
						baseInfo.content = value.toString();
						if (strs.length >= 57)
						{
							baseInfo.times = (int) (sf.parse(strs[0]).getTime() / 1000);
							baseInfo.mmeUes1apid = Long.parseLong(strs[56]);
							outname = "gmos";
						}
						else if (strs.length >= 11)
						{
							baseInfo.times = (int) (sf.parse(strs[1]).getTime() / 1000);
							baseInfo.mmeUes1apid = Long.parseLong(strs[11]);
							outname = "wjtdh";
						}

						if (xdrLableMng.dealGLBaseInfo(baseInfo))
						{
							curText.set(value.toString() + "," + baseInfo.imsi);
							mosMng.write(outname, NullWritable.get(), curText);
						}
					}
					catch (Exception e)
					{
					}
				}
			}
		}
	}

}
