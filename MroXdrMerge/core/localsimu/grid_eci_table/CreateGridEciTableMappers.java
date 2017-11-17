package localsimu.grid_eci_table;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import jan.com.hadoop.mapred.DataDealMapper;
import localsimu.cellgrid_merge.CellGridTimeKey;
import mroxdrmerge.MainModel;

public class CreateGridEciTableMappers
{
	public static class CellGridMap extends DataDealMapper<Object, Text, GridKey, Text>
	{
		private int longitude;
		private int latitude;
		private long eci;
		private Text curText = new Text();

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split(",|\t", -1);
			if (valstrs.length < 3)
			{
				return;
			}
			longitude = Integer.parseInt(valstrs[0]);
			latitude = Integer.parseInt(valstrs[1]);
			eci = Long.parseLong(valstrs[2]);
			GridKey gridKey = new GridKey(longitude, latitude);
			curText.set(eci + "");
			context.write(gridKey, curText);
		}
	}

	public static class figureMap extends DataDealMapper<Object, Text, GridKey, Text>
	{
		private int longitude;
		private int latitude;
		private long eci;
		private int size = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSize());
		private Text curText = new Text();

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split(",|\t", -1);
			GridKey gridKey = null;
			if (valstrs.length < 8)
			{
				return;
			}
			if (!valstrs[0].equals("-1"))// building 数据不参与校准
			{
				return;
			}
			eci = Long.parseLong(valstrs[4]);
			if (size == 10)
			{
				longitude = Integer.parseInt(valstrs[1]);
				latitude = Integer.parseInt(valstrs[2]);
				gridKey = new GridKey(longitude / 1000 * 1000, latitude / 900 * 900 + 900);
			}
			else if (size == 40)
			{
				longitude = Integer.parseInt(valstrs[1]);
				latitude = Integer.parseInt(valstrs[2]);
				gridKey = new GridKey(longitude / 4000 * 4000, latitude / 3600 * 3600 + 3600);
			}
			curText.set(eci + "");
			context.write(gridKey, curText);
		}
	}

	public static class GridEciPartitioner extends Partitioner<GridKey, Text> implements Configurable
	{
		@Override
		public Configuration getConf()
		{
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setConf(Configuration arg0)
		{
			// TODO Auto-generated method stub
		}

		@Override
		public int getPartition(GridKey key, Text value, int numOfReducer)
		{
			return Math.abs(String.valueOf(key.getLongitude() + "_" + key.getLatitude()).hashCode()) % numOfReducer;
		}
	}

}
