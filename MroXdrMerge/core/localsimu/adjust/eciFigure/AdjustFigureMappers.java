package localsimu.adjust.eciFigure;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import jan.com.hadoop.mapred.DataDealMapper;

public class AdjustFigureMappers
{
	public static class EciTableConfigMap extends DataDealMapper<Object, Text, Text, Text>
	{
		private Text curText = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String eci = value.toString().trim();
			if (eci.length() < 0)
			{
				return;
			}
			curText.set(eci);
			context.write(curText, value);
		}
	}
}
