package xdr.lablefill;

import java.util.HashMap;

import imsiCellTime.Merge.ImeiCellTimesKey;

public class LocationImsiCellLocMng
{
	private HashMap<ImeiCellTimesKey, LocationImsiCellTime> ImsiCellLocMap = null;
	private long imsi = 0;

	public void cleanList()
	{
		if (ImsiCellLocMap != null)
		{
			ImsiCellLocMap.clear();
		}
	}

	public void setImsi(long imsi)
	{
		if (imsi > 0)
		{
			this.imsi = imsi;
		}
	}

	public LocationImsiCellLocMng()
	{
		ImsiCellLocMap = new HashMap<ImeiCellTimesKey, LocationImsiCellTime>();
	}

	public HashMap<ImeiCellTimesKey, LocationImsiCellTime> getImsiCellLocMap()
	{
		return ImsiCellLocMap;
	}

	public long getImsi()
	{
		return imsi;
	}

	public LocationImsiCellTime getItem(ImeiCellTimesKey key)
	{
		return ImsiCellLocMap.get(key);
	}

	public void putItem(LocationImsiCellTime item)
	{
		ImeiCellTimesKey key = new ImeiCellTimesKey(item.Imsi, item.dayhour, item.eci);
		if (!ImsiCellLocMap.containsKey(key))
		{
			ImsiCellLocMap.put(key, item);
		}
	}
}
