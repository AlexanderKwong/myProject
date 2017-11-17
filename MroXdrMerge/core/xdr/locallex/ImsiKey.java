package xdr.locallex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ImsiKey implements WritableComparable<ImsiKey>
{
	private long imsi;
	private int dataType;// 1 locinfo; 2 s1u-http

	// ZhaiKaiShun
	private long s1apid;
	private long eci;

	public long getS1apid()
	{
		return s1apid;
	}

	public void setS1apid(long s1apid)
	{
		this.s1apid = s1apid;
	}

	public long getEci()
	{
		return eci;
	}

//	public void setEci(String eci)
//	{
//		this.eci = eci;
//	}

	// 要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public ImsiKey()
	{
	}

	public ImsiKey(long imsi, int dataType, long s1apid, long eci)
	{
		this.imsi = imsi;
		this.dataType = dataType;
		this.s1apid = s1apid;
		this.eci = eci;
	}

	public long getImsi()
	{
		return imsi;
	}

	public int getDataType()
	{
		return dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.imsi);
		out.writeInt(this.dataType);
		out.writeLong(this.s1apid);
//		out.writeBytes(this.eci);
		
		out.writeLong(this.eci);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
		this.dataType = in.readInt();
		this.s1apid = in.readLong();
		this.eci = in.readLong();
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return imsi + "_" + dataType + "_" + s1apid + "_" + eci;
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

		if (obj instanceof ImsiKey)
		{
			ImsiKey s = (ImsiKey) obj;

			return imsi == s.getImsi() && dataType == s.getDataType() && s1apid == s.getS1apid() && eci==s.getEci();
		}
		else
		{
			return false;
		}
	}

	// 对比到10分钟粒度就可以了，不用太细
	@Override
	public int compareTo(ImsiKey o)
	{
		// TODO o.imsi 也要大于0吧?
		if (imsi > 0 && o.imsi>0)
		{
			return compareToImsi(o);
		}
		else if(imsi <= 0 && o.imsi<=0)
		{
			return compareToS1apidAndEci(o);
		}
		else{
			return compareToImsi(o);
		}

	}

	public int compareToImsi(ImsiKey o)
	{
		if (imsi > o.imsi)
		{
			return 1;
		}
		else if (imsi < o.imsi)
		{
			return -1;
		}
		else
		{
			if (dataType > o.getDataType())
			{
				return 1;
			}
			else if (dataType < o.getDataType())
			{
				return -1;
			}
			return 0;

		}
	}

	public int compareToS1apidAndEci(ImsiKey o)
	{
		if (s1apid > o.s1apid)
		{
			return 1;
		}
		else if (s1apid < o.s1apid)
		{
			return -1;
		}
		else
		{

			
			if (eci>o.eci)
			{
				return 1;
			}
			else if (eci<o.eci)
			{
				return -1;
			}
			else
			{
				if (dataType > o.getDataType())
				{
					return 1;
				}
				else if (dataType < o.getDataType())
				{
					return -1;
				}
				return 0;
			}

		}
	}

}
