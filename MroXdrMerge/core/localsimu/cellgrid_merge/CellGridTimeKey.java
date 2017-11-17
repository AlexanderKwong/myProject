package localsimu.cellgrid_merge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CellGridTimeKey implements WritableComparable<CellGridTimeKey>
{
	private int itllongitude;
	private int itllatitude;
	private long iCi;
	private int time;

	public CellGridTimeKey()
	{

	}

	public CellGridTimeKey(int itllongitude, int itllatitude, long iCi, int time)
	{
		super();
		this.itllongitude = itllongitude;
		this.itllatitude = itllatitude;
		this.iCi = iCi;
		this.time = time;
	}

	public int getItllongitude()
	{
		return itllongitude;
	}

	public void setItllongitude(int itllongitude)
	{
		this.itllongitude = itllongitude;
	}

	public int getItllatitude()
	{
		return itllatitude;
	}

	public void setItllatitude(int itllatitude)
	{
		this.itllatitude = itllatitude;
	}

	public long getiCi()
	{
		return iCi;
	}

	public void setiCi(long iCi)
	{
		this.iCi = iCi;
	}

	public int getTime()
	{
		return time;
	}

	public void setTime(int time)
	{
		this.time = time;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.itllongitude = in.readInt();
		this.itllatitude = in.readInt();
		this.iCi = in.readInt();
		this.time = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.itllongitude);
		out.writeInt(this.itllatitude);
		out.writeLong(this.iCi);
		out.writeInt(this.time);
	}

	@Override
	public String toString()
	{
		return this.itllongitude + "_" + this.itllatitude + "_" + this.iCi + "_" + this.time;
	}

	@Override
	public int hashCode()
	{
		// TODO Auto-generated method stub
		return toString().hashCode();
	}

	public boolean ifCommonCellGrid(Object obj)
	{
		// TODO Auto-generated method stub
		if (obj == null)
		{
			return false;
		}
		if (this == obj)
		{
			return true;
		}
		if (obj instanceof CellGridTimeKey)
		{
			CellGridTimeKey temp = (CellGridTimeKey) obj;
			return this.itllatitude == temp.itllatitude && this.itllongitude == temp.itllongitude
					&& this.iCi == temp.iCi;
		}
		else
		{
			return false;
		}
	}

	@Override
	public boolean equals(Object obj)
	{
		// TODO Auto-generated method stub
		if (obj == null)
		{
			return false;
		}
		if (this == obj)
		{
			return true;
		}
		if (obj instanceof CellGridTimeKey)
		{
			CellGridTimeKey temp = (CellGridTimeKey) obj;
			return this.itllatitude == temp.itllatitude && this.itllongitude == temp.itllongitude
					&& this.iCi == temp.iCi && this.time == temp.time;
		}
		else
		{
			return false;
		}
	}

	@Override
	public int compareTo(CellGridTimeKey o)
	{
		// TODO Auto-generated method stub
		if (this.itllongitude > o.itllongitude)
		{
			return 1;
		}
		else if (this.itllongitude < o.itllongitude)
		{
			return -1;
		}
		else
		{
			if (this.itllatitude > o.itllatitude)
			{
				return 1;
			}
			else if (this.itllatitude < o.itllatitude)
			{
				return -1;
			}
			else
			{
				if (this.iCi > o.iCi)
				{
					return 1;
				}
				else if (this.iCi < o.iCi)
				{
					return -1;
				}
				else
				{
					if (o.time > this.time)
					{
						return 1;
					}
					else if (o.time < this.time)
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
