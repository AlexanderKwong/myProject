package xdr.locallex.model;

import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import StructData.GridItem;
import StructData.StaticConfig;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;

public class XdrData_Mme extends XdrDataBase
{
	private Date tmDate = new Date();
	private static ParseItem parseItem;
	private StringBuffer value;

	public int Procedure_Type;
	public int Procedure_Status;
	public int KeyWord1;
	public int Request_Cause;
	public int tac;
	public int Failure_Cause;
	public int Cell_ID;
	public int EPS_Bearer_Number;
	public int bearer_status_index; // bearer_status_index 等于多少需要写死
	public int bear_status1; // bear_status为1的个数
	public int bear_status12; // bear_status 为1 或者2的个数
	public int bear_statusover1; // bear_status大于1的个数

	// 统计事件
	private long CombinedEPS附着成功次数;
	private long CombinedEPS附着请求次数;
	private long eNB请求释放上下文数;
	private long EPS附着成功次数;
	private long EPS附着请求次数;
	private long ERAB建立成功数;
	private long ERAB建立请求数;
	private long MME内S1接口每相邻关系切出尝试次数;
	private long MME内S1接口切出成功次数;
	private long MME内S1接口切入尝试次数;
	private long MME内S1接口切入成功次数;
	private long MME内X2接口切换尝试数;
	private long MME内X2接口切换成功次数;
	private long 初始上下文建立成功次数;
	private long 跟踪区更新成功次数;
	private long 跟踪区更新请求次数;
	private long 联合跟踪区更新成功次数;
	private long 联合跟踪区更新请求次数;
	private long 寻呼记录接收个数;
	private long 遗留上下文个数;
	private long 正常的eNB请求释放上下文数;
	private long 专用承载激活成功次数;
	private long 专用承载激活请求次数;

	// 异常 事件
	private long CombinedEPS附着失败;
	private long EPS附着失败;
	private long ERAB建立失败;
	private long MME内S1接口切出失败;
	private long MME内S1接口切入失败;
	private long MME内X2接口切换失败;
	private long 初始上下文建立失败;
	private long eNB请求释放上下文失败;
	private long 跟踪区更新失败;
	private long 联合跟踪区更新失败;
	private long 非正常的eNB请求释放上下文;
	private long 专用承载激活失败;

	// 成功事件
	private long CombinedEPS附着成功;
	private long EPS附着成功;
	private long ERAB建立成功;
	private long MME内S1接口切出成功;
	private long MME内S1接口切入成功;
	private long MME内X2接口切换成功;
	private long 初始上下文建立成功;
	private long eNB请求释放成功上下文数;
	private long 跟踪区更新成功;
	private long 联合跟踪区更新成功;
	private long 正常的eNB请求释放上下文;
	private long 专用承载激活成功;

	public XdrData_Mme()
	{
		super();
		clear();

		if (parseItem == null)
		{
			parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-mme");
		}
	}

	public void clear()
	{
		value = new StringBuffer();
	}

	@Override
	public ParseItem getDataParseItem() throws IOException
	{
		return parseItem;
	}

	@Override
	public boolean FillData_short(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

		}
		catch (Exception e)
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
			{
				e.printStackTrace();
			}
			return false;
		}

		return true;
	}

	@Override
	public boolean FillData(DataAdapterReader dataAdapterReader) throws ParseException, IOException
	{
		try
		{
			tmDate = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
			istime = (int) (tmDate.getTime() / 1000L);
			istimems = (int) (tmDate.getTime() % 1000L);
			// etime
			tmDate = dataAdapterReader.GetDateValue("Procedure_End_Time", new Date(1970, 1, 1));
			ietime = (int) (tmDate.getTime() / 1000L);
			ietimems = (int) (tmDate.getTime() % 1000L);

			imsi = dataAdapterReader.GetLongValue("IMSI", 0);

			value = dataAdapterReader.getTmStrs();

			Procedure_Type = dataAdapterReader.GetIntValue("Procedure_Type", StaticConfig.Int_Abnormal);
			Procedure_Status = dataAdapterReader.GetIntValue("Procedure_Status", StaticConfig.Int_Abnormal);
			KeyWord1 = dataAdapterReader.GetIntValue("KEYWORD1", StaticConfig.Int_Abnormal);// no
			Request_Cause = dataAdapterReader.GetIntValue("REQUEST_CAUSE", StaticConfig.Int_Abnormal);// no
			// 用不上 tac= dataAdapterReader.GetIntValue("TAC",
			// StaticConfig.Int_Abnormal);
			Failure_Cause = dataAdapterReader.GetIntValue("FAILURE_CAUSE", StaticConfig.Int_Abnormal);// no
			Cell_ID = dataAdapterReader.GetIntValue("Cell_ID", StaticConfig.Int_Abnormal);

			/**
			 * TODO 2017-11-07 需要做成可配置的
			 */
			List<Integer> BEARERIDList = new ArrayList<>();
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi))
			{
				BEARERIDList = Arrays.asList(38, 46, 54, 62, 70, 78);
			}
			// if(MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan)){
			// BEARERIDList = Arrays.asList(38,46,54,62,70,78);
			// }

			for (int i = 0; i < BEARERIDList.size(); i++)
			{
				try
				{
					int Bearer_num = Integer.parseInt(dataAdapterReader.tmStrs[BEARERIDList.get(i)]);
					if (Bearer_num == 1)
					{
						bear_status1++;
					}
					if (Bearer_num == 1 || Bearer_num == 2)
					{
						bear_status12++;
					}
					if (Bearer_num > 1)
					{
						bear_statusover1++;
					}
				}
				catch (Exception e)
				{
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
					{
						e.printStackTrace();
					}
					// 不处理任何事情
				}

			}

			// value = dataAdapterReader.getTmStrs();
		}
		catch (Exception e)
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
			{
				e.printStackTrace();
			}
			return false;
		}

		return true;
	}

	@Override
	public ArrayList<EventData> toEventData()
	{
		ArrayList<EventData> eventDataLists = new ArrayList<EventData>();
		EventData eventData = new EventData();
		// 统计事件：
		boolean haveEventStat = false;
		if (Procedure_Type == 1 && KeyWord1 == 2 && Procedure_Status == 0)
		{
			CombinedEPS附着成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 1 && KeyWord1 == 2)
		{
			CombinedEPS附着请求次数 = 1;
			haveEventStat = true;
		}
		// 2017-10-27 把 Procedure_Type == 20 改成 Procedure_Type == 19
		if (Procedure_Type == 19 && Procedure_Status == 0)
		{
			eNB请求释放上下文数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 1 && Procedure_Status == 0)
		{
			EPS附着成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 1)
		{
			EPS附着请求次数 = 1;
			haveEventStat = true;
		}

		if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
				&& bear_status1 > 0)
		{
			ERAB建立成功数 = bear_status1;
			haveEventStat = true;
		}
		if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
				&& bear_status12 > 0)
		{
			ERAB建立请求数 = bear_status12;
			haveEventStat = true;
		}
		if (Procedure_Type == 15)
		{
			MME内S1接口每相邻关系切出尝试次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 15 && Procedure_Status == 0)
		{
			MME内S1接口切出成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 16)
		{
			MME内S1接口切入尝试次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 16 && Procedure_Status == 0)
		{
			MME内S1接口切入成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 14)
		{
			MME内X2接口切换尝试数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 14 && Procedure_Status == 0)
		{
			MME内X2接口切换成功次数 = 1;
			haveEventStat = true;
		}
		if ((Procedure_Type == 7 || Procedure_Type == 17) && Procedure_Status == 0)
		{
			初始上下文建立成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 5 && Procedure_Status == 0)
		{
			跟踪区更新成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 5)
		{
			跟踪区更新请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2) && Procedure_Status == 0)
		{
			联合跟踪区更新成功次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2))
		{
			联合跟踪区更新请求次数 = 1;
			haveEventStat = true;
		}
		if (Procedure_Type == 4)
		{
			寻呼记录接收个数 = 1;
			haveEventStat = true;
		}

		/**
		 * TODO zhaikaishun 2017-09-26 [2017-09-26] 遗留上下文个数
		 */
		// kpiset为 1 的判断是否有数据
		boolean haveEventStat1 = false;
		if (Procedure_Type == 20 && (Request_Cause == 20 || Request_Cause == 23 || Request_Cause == 27
				|| Request_Cause == 24 || Request_Cause == 36))
		{
			正常的eNB请求释放上下文数 = 1;
			haveEventStat1 = true;
		}
		if (Procedure_Type == 13 && Procedure_Status == 0)
		{
			专用承载激活成功次数 = 1;
			haveEventStat1 = true;
		}
		if (Procedure_Type == 13)
		{
			专用承载激活请求次数 = 1;
			haveEventStat1 = true;
		}

		eventData.iCityID = iCityID;
		eventData.IMSI = imsi;
		eventData.iEci = Cell_ID;
		eventData.iTime = istime;
		eventData.wTimems = istimems; // 开始时间里面的毫秒
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = iLongitude;
		eventData.iLatitude = iLatitude;
		eventData.iBuildID = ibuildid;
		eventData.iHeight = iheight;
		eventData.Interface = StaticConfig.INTERFACE_MME;
		eventData.iKpiSet = 1;
		eventData.iProcedureType = Procedure_Type;
		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		eventData.eventStat.fvalue[0] = CombinedEPS附着成功次数;
		eventData.eventStat.fvalue[1] = CombinedEPS附着请求次数;
		eventData.eventStat.fvalue[2] = EPS附着成功次数;
		eventData.eventStat.fvalue[3] = EPS附着请求次数;
		eventData.eventStat.fvalue[4] = ERAB建立成功数;
		eventData.eventStat.fvalue[5] = ERAB建立请求数;
		eventData.eventStat.fvalue[6] = MME内S1接口每相邻关系切出尝试次数;
		eventData.eventStat.fvalue[7] = MME内S1接口切出成功次数;
		eventData.eventStat.fvalue[8] = MME内S1接口切入尝试次数;
		eventData.eventStat.fvalue[9] = MME内S1接口切入成功次数;
		eventData.eventStat.fvalue[10] = MME内X2接口切换尝试数;
		eventData.eventStat.fvalue[11] = MME内X2接口切换成功次数;
		eventData.eventStat.fvalue[12] = 初始上下文建立成功次数;
		eventData.eventStat.fvalue[13] = eNB请求释放上下文数;
		eventData.eventStat.fvalue[14] = 跟踪区更新成功次数;
		eventData.eventStat.fvalue[15] = 跟踪区更新请求次数;
		eventData.eventStat.fvalue[16] = 联合跟踪区更新成功次数;
		eventData.eventStat.fvalue[17] = 联合跟踪区更新请求次数;
		eventData.eventStat.fvalue[18] = 遗留上下文个数;
		eventData.eventStat.fvalue[19] = 寻呼记录接收个数;

		EventData eventData1 = new EventData();

		eventData1.iCityID = iCityID;
		eventData1.IMSI = imsi;
		eventData1.iEci = Cell_ID;
		eventData1.iTime = istime;
		eventData1.wTimems = istimems; // 开始时间里面的毫秒
		eventData1.strLoctp = strloctp;
		eventData1.strLabel = label;
		eventData1.iLongitude = iLongitude;
		eventData1.iLatitude = iLatitude;
		eventData1.iBuildID = ibuildid;
		eventData1.iHeight = iheight;
		eventData1.Interface = StaticConfig.INTERFACE_MME;
		eventData1.iKpiSet = 2;
		eventData1.iProcedureType = Procedure_Type;
		eventData1.iTestType = testType;
		eventData1.iDoorType = iDoorType;
		eventData1.iLocSource = locSource;
		eventData1.confidentType = confidentType;
		eventData1.iAreaType = iAreaType;
		eventData1.iAreaID = iAreaID;

		eventData1.eventStat.fvalue[0] = 正常的eNB请求释放上下文数;
		eventData1.eventStat.fvalue[1] = 专用承载激活成功次数;
		eventData1.eventStat.fvalue[2] = 专用承载激活请求次数;

		// 异常事件
		String exceptionDetail = "";

		if ("yes".equals(MainModel.GetInstance().getAppConfig().getIfOutFailData()))
		{
			if (Procedure_Type == 1 && KeyWord1 == 2 && Procedure_Status > 0)
			{
				CombinedEPS附着失败 = 1;
				exceptionDetail = "CombinedEPS附着失败";
			}

			if (Procedure_Type == 1 && KeyWord1 != 2 && Procedure_Status > 0)
			{
				EPS附着失败 = 1;
				exceptionDetail = "EPS附着失败";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
					&& bear_statusover1 > 0)
			{
				ERAB建立失败 = bear_statusover1;
				exceptionDetail = "ERAB建立失败";
			}
			if (Procedure_Type == 15 && Procedure_Status > 0)
			{
				MME内S1接口切出失败 = 1;
				exceptionDetail = "MME内S1接口切出失败";
			}
			if (Procedure_Type == 16 && Procedure_Status > 0)
			{
				MME内S1接口切入失败 = 1;
				exceptionDetail = "MME内S1接口切入失败";
			}
			if (Procedure_Type == 14 && Procedure_Status > 0)
			{
				MME内X2接口切换失败 = 1;
				exceptionDetail = "MME内X2接口切换失败";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 17) && Procedure_Status > 0)
			{
				初始上下文建立失败 = 1;
				exceptionDetail = "初始上下文建立失败";
			}
			if (Procedure_Type == 20 && Procedure_Status > 0)
			{
				eNB请求释放上下文失败 = 1;
				exceptionDetail = "eNB请求释放上下文失败";
			}
			if (Procedure_Type == 5 && (KeyWord1 != 1 && KeyWord1 != 2) && Procedure_Status > 0)
			{
				跟踪区更新失败 = 1;
				exceptionDetail = "跟踪区更新失败";
			}
			if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2) && Procedure_Status > 0)
			{
				联合跟踪区更新失败 = 1;
				exceptionDetail = "联合跟踪区更新失败";
			}
			if (Procedure_Type == 20 && (Request_Cause != 20 && Request_Cause != 23 && Request_Cause != 27
					&& Request_Cause != 24 && Request_Cause != 36))
			{
				非正常的eNB请求释放上下文 = 1;
				exceptionDetail = "非正常的eNB请求释放上下文";
			}
			if (Procedure_Type == 13 && Procedure_Status > 0)
			{
				专用承载激活失败 = 1;
				exceptionDetail = "专用承载激活失败";
			}
		}

		// 成功事件
		String exceptionDetaiSuccess = "";

		// 判断是否需要吐出
		if ("yes".equals(MainModel.GetInstance().getAppConfig().getIfOutSuccussData()))
		{
			if (Procedure_Type == 1 && KeyWord1 == 2 && Procedure_Status <= 0)
			{
				CombinedEPS附着成功 = 1;
				exceptionDetaiSuccess = "CombinedEPS附着成功";
			}

			if (Procedure_Type == 1 && KeyWord1 != 2 && Procedure_Status <= 0)
			{
				EPS附着成功 = 1;
				exceptionDetaiSuccess = "EPS附着成功";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 9 || Procedure_Type == 10 || Procedure_Type == 13)
					&& bear_statusover1 <= 0) // 是不是这样 TODO 2017-10-09
			{
				ERAB建立成功数 = bear_statusover1;
				exceptionDetaiSuccess = "ERAB建立成功数";
			}
			if (Procedure_Type == 15 && Procedure_Status <= 0)
			{
				MME内S1接口切出成功 = 1;
				exceptionDetaiSuccess = "MME内S1接口切出成功";
			}
			if (Procedure_Type == 16 && Procedure_Status <= 0)
			{
				MME内S1接口切入成功 = 1;
				exceptionDetaiSuccess = "MME内S1接口切入成功";
			}
			if (Procedure_Type == 14 && Procedure_Status <= 0)
			{
				MME内X2接口切换成功 = 1;
				exceptionDetaiSuccess = "MME内X2接口切换成功";
			}
			if ((Procedure_Type == 7 || Procedure_Type == 17) && Procedure_Status <= 0)
			{
				初始上下文建立成功 = 1;
				exceptionDetaiSuccess = "初始上下文建立成功";
			}
			if (Procedure_Type == 20 && Procedure_Status <= 0)
			{
				eNB请求释放成功上下文数 = 1;
				exceptionDetaiSuccess = "eNB请求释放成功上下文数";
			}
			if (Procedure_Type == 5 && (KeyWord1 != 1 && KeyWord1 != 2) && Procedure_Status <= 0)
			{
				跟踪区更新成功 = 1;
				exceptionDetaiSuccess = "跟踪区更新成功";
			}
			if (Procedure_Type == 5 && (KeyWord1 == 1 || KeyWord1 == 2) && Procedure_Status <= 0)
			{
				联合跟踪区更新成功 = 1;
				exceptionDetaiSuccess = "联合跟踪区更新成功";
			}
			// todo 是不是这样
			if (Procedure_Type == 20 && (Request_Cause == 20 || Request_Cause == 23 || Request_Cause == 27
					|| Request_Cause == 24 || Request_Cause == 36))
			{
				正常的eNB请求释放上下文 = 1;
				exceptionDetaiSuccess = "正常的eNB请求释放上下文";
			}
			if (Procedure_Type == 13 && Procedure_Status <= 0)
			{
				专用承载激活成功 = 1;
				exceptionDetaiSuccess = "专用承载激活成功";
			}
		}

		if (!haveEventStat)
		{
			eventData.eventStat = null;
		}
		eventDataLists.add(eventData);

		if (exceptionDetail.length() > 0)
		{
			eventData.eventDetial.strvalue[0] = exceptionDetail;
			eventData.eventDetial.fvalue[0] = LteScRSRP;
			eventData.eventDetial.fvalue[1] = LteScSinrUL;
			eventData.eventDetial.fvalue[2] = CombinedEPS附着失败;
			eventData.eventDetial.fvalue[3] = EPS附着失败;
			eventData.eventDetial.fvalue[4] = ERAB建立失败;
			eventData.eventDetial.fvalue[5] = MME内S1接口切出失败;
			eventData.eventDetial.fvalue[6] = MME内S1接口切入失败;
			eventData.eventDetial.fvalue[7] = MME内X2接口切换失败;
			eventData.eventDetial.fvalue[8] = 初始上下文建立失败;
			eventData.eventDetial.fvalue[9] = eNB请求释放上下文失败;
			eventData.eventDetial.fvalue[10] = 跟踪区更新失败;
			eventData.eventDetial.fvalue[11] = 联合跟踪区更新失败;
			eventData.eventDetial.fvalue[12] = 非正常的eNB请求释放上下文;
			eventData.eventDetial.fvalue[13] = 专用承载激活失败;
		}
		else if (exceptionDetaiSuccess.length() > 0)
		{

			eventData.eventDetial.strvalue[0] = exceptionDetaiSuccess;
			eventData.eventDetial.fvalue[0] = LteScRSRP;
			eventData.eventDetial.fvalue[1] = LteScSinrUL;
			eventData.eventDetial.fvalue[2] = CombinedEPS附着成功;
			eventData.eventDetial.fvalue[3] = EPS附着成功;
			eventData.eventDetial.fvalue[4] = ERAB建立成功数;
			eventData.eventDetial.fvalue[5] = MME内S1接口切出成功;
			eventData.eventDetial.fvalue[6] = MME内S1接口切入成功;
			eventData.eventDetial.fvalue[7] = MME内X2接口切换成功;
			eventData.eventDetial.fvalue[8] = 初始上下文建立成功;
			eventData.eventDetial.fvalue[9] = eNB请求释放成功上下文数;
			eventData.eventDetial.fvalue[10] = 跟踪区更新成功;
			eventData.eventDetial.fvalue[11] = 联合跟踪区更新成功;
			eventData.eventDetial.fvalue[12] = 正常的eNB请求释放上下文;
			eventData.eventDetial.fvalue[13] = 专用承载激活成功;
		}
		else
		{
			eventData.eventDetial = null;
		}

		if (haveEventStat1)
		{
			eventData1.eventDetial = null; // 这个detial不用输出的，所有的detial都输出到eventData里面去了
			eventDataLists.add(eventData1);
		}

		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;

		return eventDataLists;
	}

	@Override
	public void toString(StringBuffer sb)
	{

		StaticConfig.putCityNameByCityId();
		String fenge = parseItem.getSplitMark();
		if (fenge.contains("\\"))
		{
			fenge = fenge.replace("\\", "");
		}

		sb.append(value);
		sb.append(fenge);
		sb.append(iLongitude);
		sb.append(fenge);
		sb.append(iLatitude);
		sb.append(fenge);
		sb.append(iheight);
		sb.append(fenge);
		sb.append(iDoorType);
		sb.append(fenge);

		sb.append(iRadius);
		sb.append(fenge);
		GridItem gridItem = GridItem.GetGridItem(0, iLongitude, iLatitude);

		int icentLng = gridItem.getBRLongitude() / 2 + gridItem.getTLLongitude() / 2;
		int icentLat = gridItem.getBRLatitude() / 2 + gridItem.getTLLatitude() / 2;

		if (StaticConfig.cityId_Name.containsKey(iCityID))
		{
			sb.append(StaticConfig.cityId_Name.get(iCityID) + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}
		else
		{
			sb.append("nocity" + "_" + icentLng + "_" + icentLat);
			sb.append(fenge);
		}

		sb.append(-1);
		sb.append(fenge);
		sb.append(-1);

	}

}
