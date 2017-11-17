package mrstat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings("serial")
public class TypeResult implements Serializable
{
	private Map<TypeInfo, StringBuffer> resultMap;
	private TypeInfoMng typeInfoMng;

	public TypeResult(TypeInfoMng typeInfoMng)
	{
		this.typeInfoMng = typeInfoMng;

		resultMap = new HashMap<TypeInfo, StringBuffer>();
	}

	public TypeResult()
	{
	}

	// public int pushData(int type, StringBuffer dataList)
	// {
	// TypeInfo item = typeInfoMng.getTypeInfo(type);
	// if (item == null)
	// {
	// return -1;
	// }
	//
	// StringBuffer aList = resultMap.get(item);
	// if (aList == null)
	// {
	// aList = new StringBuffer();
	// resultMap.put(item, aList);
	// }
	// aList.append(dataList);
	// aList.append("\r\n");
	// return 0;
	// }

	public int pushData(int type, String data)
	{
		TypeInfo item = typeInfoMng.getTypeInfo(type);
		if (item == null)
		{
			return -1;
		}

		StringBuffer aList = resultMap.get(item);
		if (aList == null)
		{
			aList = new StringBuffer();
			resultMap.put(item, aList);
			aList.append(data);
		}
		else
		{
			aList.append("\r\n");
			aList.append(data);
		}
		return 0;
	}

	public void cleanMap()
	{
		resultMap.clear();
	}

	public Set<Entry<TypeInfo, StringBuffer>> getMapEntry()
	{
		return resultMap.entrySet();
	}
}
