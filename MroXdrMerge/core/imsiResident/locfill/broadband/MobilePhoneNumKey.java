package imsiResident.locfill.broadband;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MobilePhoneNumKey implements WritableComparable<MobilePhoneNumKey>
{
	/**
	 * TODO 此处应继承IDataType
	 * @author Kwong
	 */
	public enum DataType{
		
		RESIDENT_USER_DATA(0, "常驻用户"), BROADBAND_DATA(1, "家庭宽带");
		
		private int code;
		
		private String name;
		
		public int getCode(){
			return code;
		}
		
		public String getName(){
			return name;
		}
		
		DataType(int code, String name){
			this.code = code;
			this.name = name;
		}
		
		public static DataType fromCode(int code){
			for(DataType dataType : values()){
				if(code == dataType.getCode())
					return dataType;
			}
			throw new IllegalArgumentException();
		}
		
	}
	/**
	 * 手机号，加密/不加密
	 */
	private String phoneNum;
	
	/**
	 * 数据类型，0：常驻用户数据；1：家庭宽带数据
	 */
	private DataType dataType;
	
	public MobilePhoneNumKey(){}
	
	public MobilePhoneNumKey(String phoneNum, DataType dataType)
	{
		super();
		this.phoneNum = phoneNum;
		this.dataType = dataType;
	}
	
	public String getPhoneNum()
	{
		return phoneNum;
	}

	public void setPhoneNum(String phoneNum)
	{
		this.phoneNum = phoneNum;
	}

	public DataType getDataType()
	{
		return dataType;
	}

	public void setDataType(DataType dataType)
	{
		this.dataType = dataType;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(phoneNum);
		out.writeInt(dataType.getCode());
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.phoneNum = in.readUTF();
		this.dataType = DataType.fromCode(in.readInt());
	}

	@Override
	public int compareTo(MobilePhoneNumKey o)
	{
		int result = phoneNum.compareTo(o.phoneNum);
		if(result == 0){
			result = Integer.compare(dataType.getCode(), o.getDataType().getCode());
		}
		return result;
	}

}
