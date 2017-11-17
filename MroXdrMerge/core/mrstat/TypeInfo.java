package mrstat;

import java.io.Serializable;

public class TypeInfo implements Comparable<TypeInfo>, Serializable
{
	protected Integer type;
	protected String outPutName;

	public Integer getType()
	{
		return type;
	}

	public String getOutPutName()
	{
		return outPutName;
	}

	public TypeInfo(int type, String outPutName)
	{
		this.type = type;
		this.outPutName = outPutName;
	}

	// 这个方法需要Overrride
	@Override
	public int hashCode()
	{
		return type.hashCode();
	}

	@Override
	public int compareTo(TypeInfo o)
	{
		return type - o.type;
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

		if (obj instanceof TypeInfo)
		{
			TypeInfo s = (TypeInfo) obj;

			return type == s.type;
		}
		else
		{
			return false;
		}
	}
}
