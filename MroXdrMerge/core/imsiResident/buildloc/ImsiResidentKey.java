package imsiResident.buildloc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ImsiResidentKey implements WritableComparable<ImsiResidentKey>
{
	public long imsi;
	public long eci;

	public long getImsi()
	{
		return imsi;
	}

	public void setImsi(long imsi)
	{
		this.imsi = imsi;
	}

	public long getEci()
	{
		return eci;
	}

	public void setEci(long eci)
	{
		this.eci = eci;
	}

	public ImsiResidentKey()
	{

	}

	/**
	 * 
	 * @param imsi
	 * @param eci
	 */
	public ImsiResidentKey(long imsi, long eci)
	{
		this.imsi = imsi;
		this.eci = eci;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.imsi = in.readLong();
		this.eci = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.imsi);
		out.writeLong(this.eci);
	}

	@Override
	public int compareTo(ImsiResidentKey o)
	{
		// TODO Auto-generated method stub
		if (this.eci > o.eci)
		{
			return 1;
		}
		else if (this.eci < o.eci)
		{
			return -1;
		}
		else
		{
			if (this.imsi > o.imsi)
			{
				return 1;
			}
			else if (this.imsi < o.imsi)
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}
	}

	public String toString()
	{
		return imsi + "," + eci;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		else if (this == obj)
		{
			return true;
		}
		else if (obj instanceof ImsiResidentKey)
		{
			ImsiResidentKey s = (ImsiResidentKey) obj;
			return imsi == s.imsi && eci == s.eci;
		}
		else
		{
			return false;
		}
	}
}
