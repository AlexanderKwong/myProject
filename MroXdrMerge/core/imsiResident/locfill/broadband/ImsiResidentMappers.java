package imsiResident.locfill.broadband;

import jan.com.hadoop.mapred.DataDealMapper;
import jan.util.LOGHelper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;


public class ImsiResidentMappers
{
	public static class ImsiResidentMapper extends DataDealMapper<Object, Text, MobilePhoneNumKey, Text>{
		
		MobilePhoneNumKey outKey = new MobilePhoneNumKey();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//imsi，小时（0-23），eci，停留时长，经度，纬度，手机号，白天晚上（0：白天 1：晚上）、buildid、楼宇经度、楼宇纬度
			String[] strs = value.toString().split(",", -1);
			if(strs.length < 7)
			{
				return;
			}
			outKey.setPhoneNum(strs[6]);
			outKey.setDataType(MobilePhoneNumKey.DataType.RESIDENT_USER_DATA);
			context.write(outKey, value);
			
		}
	}
	
	public static class BroadBandMapper extends DataDealMapper<Object, Text, MobilePhoneNumKey, Text>{
		
		MobilePhoneNumKey outKey = new MobilePhoneNumKey();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//手机号码 、经度、纬度、楼层、楼宇id
			String[] strs = value.toString().split(",", -1);
			if(strs.length < 5)
			{
				return;
			}
			outKey.setPhoneNum(strs[0]);
			outKey.setDataType(MobilePhoneNumKey.DataType.BROADBAND_DATA);
			context.write(outKey, value);
		}
	}
	
	public static class PhoneNumPartitioner extends Partitioner<MobilePhoneNumKey, Text>{

		@Override
		public int getPartition(MobilePhoneNumKey key, Text value, int numPartitions)
		{
			return Math.abs(key.getPhoneNum().hashCode()) % numPartitions;
		}
		
	}
	
	public static class PhoneNumDataTypeSortComparator extends WritableComparator{
		
		public PhoneNumDataTypeSortComparator(){
			super(MobilePhoneNumKey.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			MobilePhoneNumKey s1 = (MobilePhoneNumKey) a;
			MobilePhoneNumKey s2 = (MobilePhoneNumKey) b;
			return s1.compareTo(s2);
		}
	}
	
	/**
	 * 使手机号相同的数据进入同一reduce方法
	 * @author Kwong
	 */
	public static class PhoneNumGroupingComparator extends WritableComparator{
		public PhoneNumGroupingComparator(){
			super(MobilePhoneNumKey.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			MobilePhoneNumKey s1 = (MobilePhoneNumKey) a;
			MobilePhoneNumKey s2 = (MobilePhoneNumKey) b;
			return s1.getPhoneNum().compareTo(s2.getPhoneNum());
		}
	}
	
	public static class ImsiResidentBloomFilterMapper extends DataDealMapper<Object, Text, NullWritable, BloomFilter>{

		BloomFilter bf = null;
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, BloomFilter>.Context context)
				throws IOException, InterruptedException
		{
			String[] strs = value.toString().split(",", -1);
			if(strs.length < 7)
			{
				return;
			}
			Key bfKey = new Key(strs[6].getBytes());
			bf.add(bfKey);
		}

		@Override
		protected void cleanup(Mapper<Object, Text, NullWritable, BloomFilter>.Context context) throws IOException,
				InterruptedException
		{
			super.cleanup(context);
//			LOGHelper.GetLogger().writeLog(LogType.info, "bloomFilter.size = " );
			context.write(NullWritable.get(), bf);
		}

		@Override
		protected void setup(Mapper<Object, Text, NullWritable, BloomFilter>.Context context) throws IOException,
				InterruptedException
		{
			super.setup(context);
			bf = new BloomFilter(0, 0, 0);
		}
		
	}
	
	public static class BroadBandBloomFilterMapper extends DataDealMapper<Object, Text, NullWritable, BloomFilter>{

		BloomFilter bf = null;
		private int counter = 0;
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, BloomFilter>.Context context)
				throws IOException, InterruptedException
		{
			String[] strs = value.toString().split(",", -1);
			if(strs.length < 5)
			{
				return;
			}
			Key bfKey = new Key(strs[0].getBytes());
			bf.add(bfKey);
		}

		@Override
		protected void cleanup(Mapper<Object, Text, NullWritable, BloomFilter>.Context context) throws IOException,
				InterruptedException
		{
			super.cleanup(context);
			LOGHelper.GetLogger().writeLog(LogType.info, "bloomFilter.size = " + counter);
			context.write(NullWritable.get(), bf);
		}

		@Override
		protected void setup(Mapper<Object, Text, NullWritable, BloomFilter>.Context context) throws IOException,
				InterruptedException
		{
			super.setup(context);
			//计算vectorsize
			//获取输入的大小
			
			bf = new BloomFilter(0, 0, Hash.MURMUR_HASH);
		}
		
	}
}
