package xdr.msisdn_imsi;

import java.io.IOException;

import jan.com.hadoop.mapred.DataDealMapper;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;


public class MsisdnJoinMapper
{

	public static class UserInfoMapper extends DataDealMapper<Object, Text, MsisdnKey, Text>{
		Text curText = new Text();
		@Override
		protected void setup(Mapper<Object, Text, MsisdnKey, Text>.Context context) throws IOException,
				InterruptedException
		{
			// TODO Auto-generated method stub
			super.setup(context);
			curText = new Text();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// 提取出 msisdnkey imsi
			try{
				String[] userArys = value.toString().split("\t",7);
				String imsi = userArys[1]; //第一位是imsi
				String imsiString = userArys[5];
				if(imsiString.startsWith("86")){
					imsiString=imsiString.substring(2);
				}
				long msisdn = Long.parseLong(imsiString); //第五位是msisdn
				
				MsisdnKey  msisdnKey = new MsisdnKey(msisdn,1);
				curText.set(imsi);
				context.write(msisdnKey, curText);
			}catch(Exception e){
				
			}

		}
	}
	
	public static class MsisdnMapper extends DataDealMapper<Object, Text, MsisdnKey, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String[] msisdnData = value.toString().split(",",5); 
				String misidnString=msisdnData[3];
				if(misidnString.startsWith("86")){
					misidnString = misidnString.substring(2);
				}
				long msisdn = Long.parseLong(misidnString);
				MsisdnKey  msisdnKey = new MsisdnKey(msisdn,100);
				context.write(msisdnKey, value);
			}catch(Exception e){
				//
			}
			
		}
	}
	
	
	public static class MsisdnSortKeyGroupComparator extends WritableComparator
	{
		public MsisdnSortKeyGroupComparator()
		{
			super(MsisdnKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			MsisdnKey s1 = (MsisdnKey) a;
			MsisdnKey s2 = (MsisdnKey) b;

			if (s1.getMsisdn() > s2.getMsisdn())
			{
				return 1;
			}
			else if (s1.getMsisdn() < s2.getMsisdn())
			{
				return -1;
			}
			else
			{
				if (s1.getDataType() > s2.getDataType())
				{
					return 1;
				}
				else if (s1.getDataType() < s2.getDataType())
				{
					return -1;
				}
				return 0;
			}

		}

	}
	
	public static class MsisdnPartitioner extends Partitioner<MsisdnKey, Text> implements Configurable
	{
		private Configuration conf = null;

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public void setConf(Configuration conf)
		{
			this.conf = conf;
		}

		@Override
		public int getPartition(MsisdnKey key, Text value, int numOfReducer)
		{
			return Math.abs(("" + key.getMsisdn()).hashCode()) % numOfReducer;
		}

	}
	
	public static class MsisdnSortKeyComparator extends WritableComparator
	{
		public MsisdnSortKeyComparator()
		{
			super(MsisdnKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			MsisdnKey s1 = (MsisdnKey) a;
			MsisdnKey s2 = (MsisdnKey) b;
			return s1.compareTo(s2);
		}

	}
	
}
