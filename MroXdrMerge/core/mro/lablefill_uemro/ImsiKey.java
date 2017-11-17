package mro.lablefill_uemro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ImsiKey implements WritableComparable<ImsiKey>
{
	private long imsi = 0;
	private int dataType = 0;// 1 xdrloc ;2 mrodata

	// 要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public ImsiKey()
	{
	}

	public ImsiKey(long imsi, int dataType)
	{
		super();
		this.imsi = imsi;
		this.dataType = dataType;
	}

	public long getImsi()
	{
		return imsi;
	}

	public void setImsi(long imsi)
	{
		this.imsi = imsi;
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
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
		this.dataType = in.readInt();
	}

	/**
	 * We want sort in descending count and descending avgts，
	 * Java里面排序默认小的放前面，即返回-1的放前面，这里直接把小值返回1，就会被排序到后面了。
	 * 
	 * Mro Data late about 20m to XDR Data
	 * 
	 */
	@Override
	public int compareTo(ImsiKey o)
	{
		if (imsi > o.getImsi())
		{
			return 1;
		}
		else if (imsi < o.getImsi())
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
			else
			{
				return 0;
			}
		}
	}

	// 这个方法需要Overrride
	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return imsi + "_" + dataType;
	}

	// 这个方法，写不写都不会影响的，至少我测的是这样
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

			return imsi == s.getImsi() && dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

}
