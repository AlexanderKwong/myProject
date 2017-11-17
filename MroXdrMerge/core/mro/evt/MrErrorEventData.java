package mro.evt;

import java.util.Arrays;

/**
 * 故障事件
 * @author Kwong
 */
public class MrErrorEventData extends EventData
{
	/**
	 * 故障 枚举
	 * 将故障 名字 & 判断标准 封装
	 */
	public enum ErrorType
	{
		SINGLE_PASS("单通"), INTERRUPT("断续");

		private String name;

		ErrorType(String name)
		{
			this.name = name;
		}

		public static ErrorType fromQCI_1(double ulQci1, double dlQci1)
		{
			if (ulQci1 >= 80 || dlQci1 >= 80)
			{
				return SINGLE_PASS;
			}
			else if ((ulQci1 >= 20 && ulQci1 < 80) || (dlQci1 >= 20 && dlQci1 < 80))
			{
				return INTERRUPT;
			}
			else throw new RuntimeException("非异常事件");
		}
		
		public static boolean isErr(double ulQci1, double dlQci1){
			if (ulQci1 < 20 && dlQci1 < 20)
				return false;
			else return true;
		}
		
		public String getName(){
			return name;
		}
	}

	public MrErrorEventData()
	{
		super();
		Interface = 0;
		iKpiSet = 1;
		iProcedureType = 1;
		//默认 qci值为0
		Arrays.fill(eventDetial.fvalue, 0D);
	}

}
