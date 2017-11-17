package localsimu.eci_cellgrid_table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class GridKey implements WritableComparable<GridKey>
{
	private int longitude;
	private int latitude;
	private int dataType;

	public GridKey()
	{
	}

	public GridKey(int longitude, int latitude, int dataType)
	{
		this.longitude = longitude;
		this.latitude = latitude;
		this.dataType = dataType;
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

	public int getDataType()
	{
		return dataType;
	}

	public void setDataType(int dataType)
	{
		this.dataType = dataType;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		this.longitude = in.readInt();
		this.latitude = in.readInt();
		this.dataType = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(this.longitude);
		out.writeInt(this.latitude);
		out.writeInt(this.dataType);
	}

	@Override
	public String toString()
	{
		return this.longitude + "_" + this.latitude + "_" + this.dataType;
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
				if (this.dataType > o.dataType)
				{
					return 1;
				}
				else if (this.dataType < o.dataType)
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
