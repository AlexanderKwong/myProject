package xdr.locallex.model;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import StructData.StaticConfig;
import jan.util.DataAdapterConf.ParseItem;
import mroxdrmerge.MainModel;
import jan.util.DataAdapterReader;
import xdr.locallex.EventData;

public class XdrData_Cdr_Sv extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;

	private int TRANS_TYPE;
	private int HO_REFER_NUMBER;
	private int PROC_TYPE;
	private String SEC_TRANS_TYPE;
	private String TRANS_SUCCED_FLAG;


	// 统计指标
	private int SRVCC切换请求次数;
	private int SRVCC失败次数;

	public XdrData_Cdr_Sv()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-CDR-SV");
		}
	}

	public void clear()
	{
		// TODO
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try{
			tmDate = dataAdapterReader.GetDateValue("PROC_STARTTIME", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			
			
			TRANS_TYPE = dataAdapterReader.GetIntValue("TRANS_TYPE", StaticConfig.Int_Abnormal);
			HO_REFER_NUMBER = dataAdapterReader.GetIntValue("HO_REFER_NUMBER", StaticConfig.Int_Abnormal);
			PROC_TYPE = dataAdapterReader.GetIntValue("PROC_TYPE", StaticConfig.Int_Abnormal);
			SEC_TRANS_TYPE = dataAdapterReader.GetStrValue("SEC_TRANS_TYPE", null);
			TRANS_SUCCED_FLAG = dataAdapterReader.GetStrValue("TRANS_SUCCED_FLAG", null);
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataList = new ArrayList<EventData>();
		if (HO_REFER_NUMBER == 3 && PROC_TYPE == 64 && TRANS_TYPE == 64)
		{
			SRVCC切换请求次数 = 1;

			if (TRANS_SUCCED_FLAG == null || !"0".equals(TRANS_SUCCED_FLAG)
					||SEC_TRANS_TYPE == null|| !"65".equals(SEC_TRANS_TYPE)) //SEC_TRANS_TYPE != 65 || SEC_TRANS_TYPE == null
			{
				SRVCC失败次数 = 1;
			}
		}
		EventData eventData = new EventData();
		eventData.eventStat.fvalue[0]=SRVCC切换请求次数;
		eventData.eventStat.fvalue[1]=SRVCC失败次数;
		if(SRVCC切换请求次数<=0 && SRVCC失败次数<=0){
			eventData.eventStat = null;
		}
		eventDataList.add(eventData);
		return eventDataList;
	}

	@Override
	public void toString(StringBuffer sb)
	{
		// TODO Auto-generated method stub

	}

}
