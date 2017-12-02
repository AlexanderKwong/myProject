package mro.lablefill;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import StructData.SIGNAL_MR_All;
import base.IDataType;
import base.IGroupKey;
import org.apache.hadoop.io.WritableComparable;

public class CellTimeKey implements WritableComparable<CellTimeKey>, IGroupKey
{
	private long eci = 0;
	private int timeSpan = 0;
	private int dataType = 0;// 1 xdrloc ;2 mrodata
	private int suTime = 0;
	public IDataType dataTypeEnum;

	// 要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
	public CellTimeKey()
	{
	}

	/**
	 *
	 * @param eci
	 * @param timeSpan
	 */
	public CellTimeKey(long eci, int timeSpan, int dataType)
	{
		super();
		this.eci = eci;
		this.timeSpan = timeSpan;
		this.dataType = dataType;
		this.dataTypeEnum = DataType.fromCode(dataType);
	}

	public CellTimeKey(long eci, int timeSpan, int dataType, int subtime)
	{
		super();
		this.eci = eci;
		this.timeSpan = timeSpan;
		this.dataType = dataType;
		this.suTime = subtime;
		this.dataTypeEnum = DataType.fromCode(dataType);
	}

	public long getEci()
	{
		return eci;
	}

	public void setEci(long eci)
	{
		this.eci = eci;
	}

	public int getTimeSpan()
	{
		return timeSpan;
	}

	public int getDataType()
	{
		return dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.eci);
		out.writeInt(this.timeSpan);
		out.writeInt(this.dataType);
		out.writeLong(this.suTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.eci = in.readLong();
		this.timeSpan = in.readInt();
		this.dataType = in.readInt();
		this.suTime = in.readInt();
		this.dataTypeEnum = DataType.fromCode(dataType);
	}

	/**
	 * We want sort in descending count and descending avgts，
	 * Java里面排序默认小的放前面，即返回-1的放前面，这里直接把小值返回1，就会被排序到后面了。
	 * 
	 * Mro Data late about 20m to XDR Data
	 * 
	 */
	@Override
	public int compareTo(CellTimeKey o)
	{
		if (eci > o.getEci())
		{
			return 1;
		}
		else if (eci < o.getEci())
		{
			return -1;
		}
		else
		{
			if (timeSpan > o.getTimeSpan())
			{
				return 1;
			}
			else if (timeSpan < o.getTimeSpan())
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
					if (dataType == 5)
					{
						if (suTime > o.suTime)
						{
							return 1;
						}
						else if (suTime < o.suTime)
						{
							return -1;
						}
						else
						{
							return 0;
						}
					}
					return 0;
				}
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
		return eci + "_" + timeSpan + "_" + dataType;
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

		if (obj instanceof CellTimeKey)
		{
			CellTimeKey s = (CellTimeKey) obj;

			return eci == s.getEci() && timeSpan == s.getTimeSpan() && dataType == s.getDataType();
		}
		else
		{
			return false;
		}
	}

	@Override
	public IDataType dataType() {
		return dataTypeEnum;
	}

	public enum DataType implements IDataType{

		XDR_LOCATION(1, "xdr_location", XdrLable.class),
		MRO(2, "mro", SIGNAL_MR_All.class),
		MRO_UNFORMATED(5, "mro_unformated", SIGNAL_MR_All.class);

		DataType(int code, String name, Class<?> clazz){
			this.code = code;
			this.name = name;
			this.modelClass = clazz;
		}

		int code;

		String name;

		Class<?> modelClass;

		@Override
		public int getCode() {
			return code;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public Class<?> getModelClass() {
			return modelClass;
		}

		public static DataType fromCode(int code){
			for (DataType dataType : values()){
				if (dataType.getCode() == code){
					return dataType;
				}
			}
			throw new IllegalArgumentException();
		}
	}
}
