package xdr.msisdn_imsi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MsisdnKey implements WritableComparable<MsisdnKey>
{
	private long msisdn;
	private int dataType;//1 locinfo; 2 s1u-http
	
    //要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public MsisdnKey() {}

	public MsisdnKey(long msisdn, int dataType)
	{
		this.msisdn = msisdn;
		this.dataType = dataType;
	}

	public long getMsisdn()
	{
		return msisdn;
	}
	
	public int getDataType()
	{
		return dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.msisdn);
		out.writeInt(this.dataType);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.msisdn = in.readLong();
		this.dataType = in.readInt();
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return msisdn + "_" + dataType;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		if (this == obj)
		{
			return true;
		}

		if (obj instanceof MsisdnKey)
		{
			MsisdnKey s = (MsisdnKey) obj;

			return msisdn == s.getMsisdn() 
					&& dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

	
	//对比到10分钟粒度就可以了，不用太细
	@Override
	public int compareTo(MsisdnKey o)
	{
		if (msisdn > o.msisdn)
		{
			return 1;
		}
		else if (msisdn < o.msisdn)
		{
			return -1;
		}
		else
		{
			if(dataType > o.getDataType())
			{
				return 1;
			}
			else if(dataType < o.getDataType())
			{
				return -1;
			}
			return 0;
			
		}
	}


}
