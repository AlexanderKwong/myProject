package localsimu.adjust.eciFigure;

public class GridKey
{
	public int longitude = 0;
	public int latitude = 0;
	public long eci = 0;

	public GridKey()
	{
	}

	public GridKey(int longitude, int latitude, long eci)
	{
		this.longitude = longitude;
		this.latitude = latitude;
		this.eci = eci;
	}

	@Override
	public String toString()
	{
		return eci + "_" + longitude + "_" + latitude;
	}

	@Override
	public int hashCode()
	{
		// TODO Auto-generated method stub
		return toString().hashCode();
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
		if (obj instanceof GridKey)
		{
			GridKey s = (GridKey) obj;
			return eci == s.eci && longitude == s.longitude && latitude == s.latitude;
		}
		else
		{
			return false;
		}
	}

}
