package localsimu.eci_cellgrid_table;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import jan.com.hadoop.mapred.DataDealMapper;

public class CreateEciCellGridTableMappers
{
	public static class EciIndexMap extends DataDealMapper<Object, Text, GridKey, Text>
	{
		private int longitude;
		private int latitude;
		private Text curText = new Text();

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valstrs = value.toString().split(":", -1);
			if (valstrs.length < 2)
			{
				return;
			}
			String tempLocation = valstrs[0];
			longitude = Integer.parseInt(tempLocation.split("_")[0]);
			latitude = Integer.parseInt(tempLocation.split("_")[1]);
			GridKey gridKey = new GridKey(longitude, latitude, 1);
			curText.set(valstrs[1]);
			context.write(gridKey, curText);
		}
	}

	public static class CellGridMap extends DataDealMapper<Object, Text, GridKey, Text>
	{
		private int longitude;
		private int latitude;

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
			if (valstrs.length < 4)
			{
				return;
			}
			longitude = Integer.parseInt(valstrs[0]);
			latitude = Integer.parseInt(valstrs[1]);
			gridKey = new GridKey(longitude, latitude, 2);
			context.write(gridKey, value);
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
