package localsimu.grid_eci_table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class GridKey implements WritableComparable<GridKey>
{
	private int longitude;
	private int latitude;

	public GridKey()
	{
	}

	public GridKey(int longitude, int latitude)
	{
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public int getLongitude()
	{
		return longitude;
	}

	public void setLongitude(int longitude)
	{
		this.longitude = longitude;
	}

	public int getLatitude()
	{
		return latitude;
	}

	public void setLatitude(int latitude)
	{
		this.latitude = latitude;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		this.longitude = in.readInt();
		this.latitude = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(this.longitude);
		out.writeInt(this.latitude);
	}

	@Override
	public String toString()
	{
		return this.longitude + "_" + this.latitude;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public int compareTo(GridKey o)
	{
		if (this.latitude > o.latitude)
		{
			return 1;
		}
		else if (this.latitude < o.latitude)
		{
			return -1;
		}
		else
		{
			if (this.longitude > o.longitude)
			{
				return 1;
			}
			else if (this.longitude < o.longitude)
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}
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
		else if (obj instanceof GridKey)
		{
			GridKey temp = (GridKey) obj;
			return this.latitude == temp.latitude && this.longitude == temp.longitude;
		}
		else
		{
			return false;
		}
	}
}
