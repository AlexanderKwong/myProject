package localsimu.adjust.eciFigure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.util.LOGHelper;
import localsimu.cellgrid_merge.CellGridStruct;
import mroxdrmerge.MainModel;

public class AdjustFigureReduce
{
	public static class AdjustFigureReducer extends DataDealReducer<Text, Text, NullWritable, Text>
	{
		protected static final Log LOG = LogFactory.getLog(AdjustFigureReducer.class);
		int size = 0;
		String figurePath = "";
		String eci_cellGridPath = "";
		FileSystem fs;
		String adjustedFigurePath = "";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			size = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSize());
			figurePath = MainModel.GetInstance().getAppConfig().getSrcFigurePath();// 要修正的指纹库所在位置
			eci_cellGridPath = MainModel.GetInstance().getAppConfig().getAdjustedSrcPath() + "/EciCellGridTable";// eci_cellgrid位置
			adjustedFigurePath = conf.get("mastercom.cellgrid.enbidtable.adjustedPath");
			try
			{
				//xsh
				fs = new HDFSOper(conf).getFs();
//				fs = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(),
//						MainModel.GetInstance().getAppConfig().getHadoopHdfsPort()).getFs();
				if (!fs.exists(new Path(adjustedFigurePath)))
				{
					fs.mkdirs(new Path(adjustedFigurePath));
				}
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// 初始化小区的信息
			if (!CellConfig.GetInstance().loadLteCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
				throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			LOGHelper.GetLogger().writeLog(LogType.info, "key:" + key.toString());
			Path eci_figurePath = null;
			Path eci_eciCellGrid = null;
			String eci = key.toString();
			if (size == 10)
			{
				eci_figurePath = new Path(figurePath + "/" + eci + ".txt.gz");
				eci_eciCellGrid = new Path(eci_cellGridPath + "/10/EciCellGridData/" + eci + ".txt");
			}
			else if (size == 40)
			{
				eci_figurePath = new Path(figurePath + "/" + eci + "_40.txt.gz");
				eci_eciCellGrid = new Path(eci_cellGridPath + "/40/EciCellGridData/" + eci + ".txt");
			}
			if (fs.exists(eci_figurePath))
			{
				adjustFigureFile(eci_figurePath, eci_eciCellGrid);
			}
		}

		public void adjustFigureFile(Path eci_figurePath, Path eci_eciCellGrid) throws IOException
		{
			String spliter = "\t";
			HashMap<GridKey, CellGridStruct> eci_cellGridMap = new HashMap<GridKey, CellGridStruct>();
			GridKey key = null;
			if (fs.exists(eci_eciCellGrid))
			{
				FSDataInputStream in = fs.open(eci_eciCellGrid);// 组织cellgrid
				BufferedReader bf = new BufferedReader(new InputStreamReader(in));
				String line = "";
				CellGridStruct cellGrid = null;
				while ((line = bf.readLine()) != null)
				{
					String temp[] = line.split(",|\t", -1);
					if (temp.length < 4)
					{
						continue;
					}
					key = new GridKey(Integer.parseInt(temp[0]), Integer.parseInt(temp[1]), Long.parseLong(temp[2]));
					cellGrid = new CellGridStruct(Integer.parseInt(temp[0]), Integer.parseInt(temp[1]),
							Long.parseLong(temp[2]), Double.parseDouble(temp[3]));
					eci_cellGridMap.put(key, cellGrid);
				}
				LOGHelper.GetLogger().writeLog(LogType.info, "eci_cellGridMap.size:" + eci_cellGridMap.size());
				bf.close();
			}
			// 修正指纹库
			FSDataInputStream fsIn = fs.open(eci_figurePath);
			BufferedReader bf = new BufferedReader(new InputStreamReader(new GZIPInputStream(fsIn)));
			FSDataOutputStream out = fs.create(new Path(adjustedFigurePath + "/" + eci_figurePath.getName()));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(out)));
			String line = "";
			Figure fig = null;
			while ((line = bf.readLine()) != null)
			{
				fig = new Figure(line);
				if (size == 10)
				{
					key = new GridKey(fig.longtitude / 1000 * 1000, fig.latitude / 900 * 900 + 900, fig.eci);
				}
				else if (size == 40)
				{
					key = new GridKey(fig.longtitude / 4000 * 4000, fig.latitude / 3600 * 3600 + 3600, fig.eci);
				}
				if (eci_cellGridMap.containsKey(key))
				{
					fig.rsrp = eci_cellGridMap.get(key).rsrp;
					eci_cellGridMap.remove(key);// 参与修正的cellgrid要删除掉
				}
				bw.write(fig.getContent() + "\r\n");// 写指纹库
			}

			StringBuffer sbf = new StringBuffer();// 扩充指纹库
			for (GridKey gKey : eci_cellGridMap.keySet())
			{
				LteCellInfo info = CellConfig.GetInstance().getLteCell(gKey.eci);
				if (info == null)
				{
					continue;
				}
				sbf.append("-1");
				sbf.append(spliter);
				if (size == 10)
				{
					sbf.append(gKey.longitude + 500);
				}
				else if (size == 40)
				{
					sbf.append(gKey.longitude + 2000);
				}
				sbf.append(spliter);
				if (size == 10)
				{
					sbf.append(gKey.latitude - 450);
				}
				else if (size == 40)
				{
					sbf.append(gKey.latitude - 1800);
				}
				sbf.append(spliter);
				sbf.append("0");
				sbf.append(spliter);
				sbf.append(gKey.eci + "");
				sbf.append(spliter);
				sbf.append(info.fcn + "");
				sbf.append(spliter);
				sbf.append(info.pci + "");
				sbf.append(spliter);
				sbf.append(eci_cellGridMap.get(gKey).rsrp);
				bw.write(sbf.toString() + "\r\n");
				sbf.delete(0, sbf.length());
			}
			bw.close();
		}
	}
}
