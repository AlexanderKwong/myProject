package localsimu.cellgrid_merge;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import jan.com.hadoop.mapred.DataDealMapper;

public class MergeCellGridMapper
{
	public static class CellGridMergeMappers extends DataDealMapper<Object, Text, CellGridTimeKey, Text>
	{
		private int itllongitude;
		private int itllatitude;
		private long iCi;
		private int time;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			String[] valstrs = value.toString().split(",|\t", -1);
			if (valstrs.length < 10)
			{
				return;
			}
			iCi = Long.parseLong(valstrs[2]);
			time = Integer.parseInt(valstrs[3]);
			itllongitude = Integer.parseInt(valstrs[8]);
			itllatitude = Integer.parseInt(valstrs[9]);
			CellGridTimeKey cellGridTimeKey = new CellGridTimeKey(itllongitude, itllatitude, iCi, time);
			context.write(cellGridTimeKey, value);
		}
	}

	public static class CellGridPartitioner extends Partitioner<CellGridTimeKey, Text> implements Configurable
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
		public int getPartition(CellGridTimeKey key, Text value, int numOfReducer)
		{
			return Math.abs(
					String.valueOf(key.getiCi() + "_" + key.getItllongitude() + "_" + key.getItllatitude()).hashCode())
					% numOfReducer;
		}
	}

	public static class CellGridGroupComparator extends WritableComparator
	{
		public CellGridGroupComparator()
		{
			super(CellGridTimeKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			CellGridTimeKey s1 = (CellGridTimeKey) a;
			CellGridTimeKey s2 = (CellGridTimeKey) b;

			if (s1.getItllongitude() > s2.getItllongitude())
			{
				return 1;
			}
			else if (s1.getItllongitude() < s2.getItllongitude())
			{
				return -1;
			}
			else
			{
				if (s1.getItllatitude() > s2.getItllatitude())
				{
					return 1;
				}
				else if (s1.getItllatitude() < s2.getItllatitude())
				{
					return -1;
				}
				else
				{
					if (s1.getiCi() > s2.getiCi())
					{
						return 1;
					}
					else if (s1.getiCi() < s2.getiCi())
					{
						return -1;
					}
					else
					{
						return 0;
					}
				}
			}
		}
	}
}
