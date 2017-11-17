package mro.lablefill_xdr_figure;

public class GridKey implements Comparable<GridKey>
{
	private int cityid = 0;
	private long tllongitude = 0;
	private long tllatitude = 0;
	private int level = -1;

	public GridKey()
	{
	}

	public void gridKey(int cityid, long tllongitude, long tllatitude)
	{
		this.cityid = cityid;
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
	}

	public GridKey(long tllongitude, long tllatitude, int level)
	{
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.level = level;
	}

	public GridKey(int cityid, long tllongitude, long tllatitude, int level)
	{
		super();
		this.cityid = cityid;
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
		this.level = level;
	}

	public GridKey(int cityid, long l, long m)
	{
		super();
		this.cityid = cityid;
		this.tllongitude = l;
		this.tllatitude = m;
	}

	public GridKey(long tllongitude, long tllatitude)
	{
		super();
		this.tllongitude = tllongitude;
		this.tllatitude = tllatitude;
	}

	public int getCityid()
	{
		return cityid;
	}

	public long getTllongitude()
	{
		return tllongitude;
	}

	public long getTllatitude()
	{
		return tllatitude;
	}

	public int getLevel()
	{
		return level;
	}

	@Override
	public String toString()
	{
		return cityid + "_" + tllongitude + "_" + tllatitude + "_" + level;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public int compareTo(GridKey key)
	{
		if (cityid > key.getCityid())
		{
			return 1;
		} else if (cityid < key.getCityid())
		{
			return -1;
		} else
		{
			if (tllongitude > key.getTllongitude())
			{
				return 1;
			} else if (tllongitude < key.getTllongitude())
			{
				return -1;
			} else
			{
				if (tllatitude > key.getTllatitude())
				{
					return 1;
				} else if (tllatitude < key.getTllatitude())
				{
					return -1;
				} else
				{
					if (level > key.getLevel())
					{
						return 1;
					} else if (level < key.getLevel())
					{
						return -1;
					} else
					{
						return 0;
					}
				}
			}
		}
	}

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

		if (obj instanceof GridKey)
		{
			GridKey s = (GridKey) obj;

			return cityid == s.cityid && tllongitude == s.tllongitude && tllatitude == s.tllatitude && level == s.level;
		} else
		{
			return false;
		}
	}

}
