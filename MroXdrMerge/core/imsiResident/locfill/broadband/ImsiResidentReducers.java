package imsiResident.locfill.broadband;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jan.com.hadoop.mapred.DataDealReducer;
import model.BroadBand;
import model.ImsiResident;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;

public class ImsiResidentReducers
{

	public static class ImsiResidentBroadbandReducer extends DataDealReducer<MobilePhoneNumKey, Text, NullWritable, Text>{
		
		Text outValue = new Text();
		
		@Override
		protected void reduce(MobilePhoneNumKey key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException
		{
			List<ImsiResident> residentUserData = new ArrayList<>(); 
			
			List<BroadBand> broadbandData = new ArrayList<>();
			
			Iterator<Text> it = values.iterator();
			while(it.hasNext() && key.getDataType() == MobilePhoneNumKey.DataType.RESIDENT_USER_DATA)
				residentUserData.add(ImsiResident.fillData(it.next().toString().split("\t", -1)));
			
			while(it.hasNext() && key.getDataType() == MobilePhoneNumKey.DataType.BROADBAND_DATA)
				broadbandData.add(BroadBand.fillData(it.next().toString().split("\t", -1)));
			
			new ImsiResidentBroadbandDeal(/*new ResultHandler()*/).deal(residentUserData, broadbandData);
			
			//由于不用输出多个目录，直接吐出
			for(ImsiResident imsiResident : residentUserData){
				outValue.set(imsiResident.toString());
				context.write(NullWritable.get(), outValue);
			}
		}
	}

	public static class ImsiResidentBroadbandBloomFilterReducer extends DataDealReducer<NullWritable, BloomFilter, NullWritable, BloomFilter>{
		
		private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";
		
		BloomFilter bf = null;
		
		@Override
		protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException,
				InterruptedException
		{
			for(BloomFilter bf1 : values){
				bf.or(bf1);
			}
		}

		@Override
		protected void setup(Reducer<NullWritable, BloomFilter, NullWritable, BloomFilter>.Context context)
				throws IOException, InterruptedException
		{
			super.setup(context);
			bf = new BloomFilter(0, 0, 0);
		}

		@Override
		protected void cleanup(Reducer<NullWritable, BloomFilter, NullWritable, BloomFilter>.Context context)
				throws IOException, InterruptedException
		{
			super.cleanup(context);
			Path outputFilePath = new Path(context.getConfiguration().get(FILTER_OUTPUT_FILE_CONF));
			FileSystem fs = FileSystem.get(context.getConfiguration());

			try (FSDataOutputStream fsdos = fs.create(outputFilePath)) {
				bf.write(fsdos);

			} catch (Exception e) {
				throw new IOException("Error while writing bloom filter to file system.", e);
			}
		}
		
		
	}

}
