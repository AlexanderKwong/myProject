package StructData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import base.IModel;
import mro.evt.EventData;
import mro.evt.MrErrorEventData;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.Func;
import util.StringUtil;

public class DT_Sample_4G implements IModel
{
	public int cityID;
	public int fileID;
	public int imeiTac;// 保存imei
	public int itime;
	public short wtimems;
	public byte bms;
	public int ilongitude;
	public int ilatitude;
	public int ispeed;// 楼宇id
	public short imode;// 楼层
	public int iLAC;
	public long iCI;
	public long Eci;
	public long IMSI;
	public String MSISDN;
	public String UETac;
	public String UEBrand;
	public String UEType;
	public int serviceType;
	public int serviceSubType;
	public String urlDomain;
	public long IPDataUL;
	public long IPDataDL;
	public int duration;
	public double IPThroughputUL;
	public double IPThroughputDL;
	public int IPPacketUL;
	public int IPPacketDL;
	public int TCPReTranPacketUL;
	public int TCPReTranPacketDL;
	public int sessionRequest;
	public int sessionResult;
	public int eventType;
	public int userType;
	public String eNBName;
	public int eNBLongitude;
	public int eNBLatitude;
	public int eNBDistance;
	public String flag;
	public int ENBId;
	public String UserLabel;
	public long CellId;
	public int Earfcn;
	public int SubFrameNbr;
	public int MmeCode;
	public int MmeGroupId;
	public long MmeUeS1apId;
	public int Weight;
	public int LteScRSRP;
	public int LteScRSRQ;
	public int LteScEarfcn;
	public int LteScPci;
	public int LteScBSR;
	public int LteScRTTD;
	public int LteScTadv;
	public int LteScAOA;
	public int LteScPHR;
	public int LteScRIP;
	public int LteScSinrUL;
	public short[] nccount;
	public NC_LTE[] tlte;
	public NC_TDS[] ttds;// imeiTac
	public NC_GSM[] tgsm;
	public NC_GSM[] tgsmall; // neimeng
	public SC_FRAME[] trip;
	public int LocFillType;// 0是正常值，1是经纬度回填

	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String loctp;
	public int indoor;

	public String networktype;
	public String lable;// static,low,unknow,high

	public int simuLongitude;
	public int simuLatitude;

	public int moveDirect;
	public String mrType;

	public int dfcnJamCellCount;// 异频干扰小区个数
	public int sfcnJamCellCount;// 同频干扰小区个数

	public int LteScPUSCHPRBNum;
	public int LteScPDSCHPRBNum;
	public int LteSceNBRxTxTimeDiff;
	// new add 前后5分钟小区切换信息
	public String eciSwitchList;
	// -------------------------------------------mr新统计分YD/LT/DX 2017.6.14

	// 新添加字段表示经纬度来源和运动状态
	public String wifilist;
	public String xdrid = "";// 关联XDRID
	public int LteScRSRP_DX; // 电信主服场强
	public int LteScRSRQ_DX; // 电信主服信号质量
	public int LteScEarfcn_DX; // 电信主服频点
	public int LteScPci_DX; // 电信主服PCI
	public int LteScRSRP_LT; // 联通主服场强
	public int LteScRSRQ_LT; // 联通主服信号质量
	public int LteScEarfcn_LT; // 联通主服频点
	public int LteScPci_LT; // 联通主服PCI

	public int locSource;// 位置精度 high /mid /low
	public int samState;// int or out

	// 重叠覆盖
	public int Overlap = 1;
	public int OverlapAll = 1;
	// 场景统计
	public int iAreaType = -1;
	public int iAreaID = -1;
	// mdt 置信度
	public int Confidence;

	public int ConfidenceType;// 置信度类型

	public GridItemOfSize grid;
	public int fullNetType;// 全网通类型
	
	public LteScPlrQciData qciData;
	
	public static final String spliter = "\t";

	public DT_Sample_4G()
	{
		nccount = new short[4];
		tlte = new NC_LTE[6];
		ttds = new NC_TDS[6];
		tgsm = new NC_GSM[6];
		tgsmall = new NC_GSM[6];
		trip = new SC_FRAME[10];
		LocFillType = 0;

		location = -1;
		dist = -1;
		radius = -1;
		loctp = "";
		indoor = -1;

		simuLongitude = 0;
		simuLatitude = 0;

		Clear();
	}

	public void Clear()
	{
		ENBId = StaticConfig.Int_Abnormal;
		CellId = StaticConfig.Long_Abnormal;
		Earfcn = StaticConfig.Int_Abnormal;
		MmeCode = StaticConfig.Int_Abnormal;
		MmeGroupId = StaticConfig.Int_Abnormal;
		MmeUeS1apId = StaticConfig.Int_Abnormal;
		Weight = StaticConfig.Int_Abnormal;
		LteScRSRP = StaticConfig.Int_Abnormal;
		LteScRSRQ = StaticConfig.Int_Abnormal;
		LteScEarfcn = StaticConfig.Int_Abnormal;
		LteScPci = StaticConfig.Int_Abnormal;
		LteScBSR = StaticConfig.Int_Abnormal;
		LteScRTTD = StaticConfig.Int_Abnormal;
		LteScTadv = StaticConfig.Int_Abnormal;
		LteScAOA = StaticConfig.Int_Abnormal;
		LteScPHR = StaticConfig.Int_Abnormal;
		LteScSinrUL = StaticConfig.Int_Abnormal;
		LteScPUSCHPRBNum = StaticConfig.Int_Abnormal;
		LteScPDSCHPRBNum = StaticConfig.Int_Abnormal;
		LteSceNBRxTxTimeDiff = StaticConfig.Int_Abnormal;

		for (int i = 0; i < nccount.length; i++)
		{
			nccount[i] = 0;
		}

		for (int i = 0; i < 6; i++)
		{
			tlte[i] = new NC_LTE();
			tlte[i].Clear();

			ttds[i] = new NC_TDS();
			ttds[i].Clear();

			tgsm[i] = new NC_GSM();
			tgsm[i].Clear();

			tgsmall[i] = new NC_GSM();
			tgsmall[i].Clear();
		}

		for (int i = 0; i < trip.length; i++)
		{
			trip[i] = new SC_FRAME();
			trip[i].Clear();
		}

		testType = -1;

		MSISDN = "";
		UETac = "";
		UEBrand = "";
		UEType = "";
		urlDomain = "";
		eNBName = "";
		flag = "";
		UserLabel = "";
		loctp = "";
		networktype = "";
		lable = "";

		moveDirect = -1;
		mrType = "";

		LocFillType = 0;

		location = -1;
		dist = -1;
		radius = -1;
		loctp = "";
		indoor = -1;

		simuLongitude = 0;
		simuLatitude = 0;

		sfcnJamCellCount = 0;
		dfcnJamCellCount = 0;
		
	};

	public boolean isOriginalLoction()
	{
		if (indoor >= 0)
		{
			return true;
		}
		return false;
	}

	public List<NC_LTE> getNclte_Freq()
	{
		List<NC_LTE> itemList = new ArrayList<NC_LTE>();
		for (int i = 0; i < 9; ++i)
		{
			NC_LTE resLte = null;
			if (i <= 4)
			{
				if (tgsm[i + 1].GsmNcellBcch > 0)
				{
					resLte = new NC_LTE();
					resLte.LteNcEarfcn = tgsm[i + 1].GsmNcellBcch;
					resLte.LteNcPci = tgsm[i + 1].GsmNcellBsic;
					resLte.LteNcRSRP = tgsm[i + 1].GsmNcellCarrierRSSI / 1000 - 200;
					resLte.LteNcRSRQ = tgsm[i + 1].GsmNcellCarrierRSSI % 1000 - 200;
				}
				else
				{
					break;
				}
			}
			else
			{
				if (ttds[i - 3].TdsNcellUarfcn > 0)
				{
					resLte = new NC_LTE();
					resLte.LteNcEarfcn = ttds[i - 3].TdsNcellUarfcn;
					resLte.LteNcPci = ttds[i - 3].TdsCellParameterId;
					resLte.LteNcRSRP = ttds[i - 3].TdsPccpchRSCP / 1000 - 200;
					resLte.LteNcRSRQ = ttds[i - 3].TdsPccpchRSCP % 1000 - 200;
				}
				else
				{
					break;
				}
			}

			if (resLte != null)
			{
				itemList.add(resLte);
			}
		}

		return itemList;
	}

	public String getEncryptIMSI()
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EncryptUser))
		{
			return StringUtil.EncryptStringToLong(IMSI + "") + "";
		}
		return IMSI + "";
	}

	public String getEncryptMSISDN()
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EncryptUser))
		{
			return StringUtil.EncryptStringToLong(MSISDN + "") + "";
		}
		return MSISDN;
	}

	public String createAreaSampleToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(cityID);
		bf.append(spliter);
		bf.append(iAreaType);
		bf.append(spliter);
		bf.append(iAreaID);
		bf.append(spliter);
		bf.append(itime);
		bf.append(spliter);
		bf.append(wtimems);
		bf.append(spliter);
		bf.append(ilongitude);
		bf.append(spliter);
		bf.append(ilatitude);
		bf.append(spliter);
		bf.append(ispeed);
		bf.append(spliter);
		bf.append(imode);
		bf.append(spliter);
		bf.append(Func.getEncryptIMSI(IMSI + ""));
		bf.append(spliter);
		bf.append(xdrid);
		bf.append(spliter);
		bf.append(UserLabel);// wifilist
		bf.append(spliter);
		bf.append(Eci / 256);
		bf.append(spliter);
		bf.append(Eci);
		bf.append(spliter);
		bf.append(MmeUeS1apId);
		bf.append(spliter);
		bf.append(imeiTac);
		bf.append(spliter);
		bf.append(LteScRSRP);
		bf.append(spliter);
		bf.append(LteScRSRQ);
		bf.append(spliter);
		bf.append(LteScEarfcn);
		bf.append(spliter);
		bf.append(LteScPci);
		bf.append(spliter);
		bf.append(LteScBSR);
		bf.append(spliter);
		bf.append(LteScRTTD);
		bf.append(spliter);
		bf.append(LteScTadv);
		bf.append(spliter);
		bf.append(LteScAOA);
		bf.append(spliter);
		bf.append(LteScPHR);
		bf.append(spliter);
		bf.append(LteScRIP);
		bf.append(spliter);
		bf.append(LteScSinrUL);
		bf.append(spliter);
		for (int i = 0; i < 3; i++)
		{
			bf.append(nccount[i]);
			bf.append(spliter);
		}
		for (NC_LTE nc_lte : tlte)
		{
			if (nc_lte.LteNcRSRP >= -150 && nc_lte.LteNcRSRP <= -30)
			{
				if (nc_lte.LteNcRSRP >= LteScRSRP - 6)
				{
					if (nc_lte.LteNcEarfcn == LteScEarfcn)
					{
						Overlap++;
					}
					OverlapAll++;
				}
			}
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append("0");
			bf.append(spliter);
		}
		for (int i = 0; i < 3; i++)
		{
			bf.append(tgsm[i].GsmNcellCarrierRSSI);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBcch);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBsic);
			bf.append(spliter);
			bf.append("0");
			bf.append(spliter);
			bf.append("0");
			bf.append(spliter);
		}
		bf.append(Overlap);
		bf.append(spliter);
		bf.append(OverlapAll);
		bf.append(spliter);
		bf.append(mrType);
		bf.append(spliter);
		bf.append(LteScRSRP_DX);
		bf.append(spliter);
		bf.append(LteScRSRQ_DX);
		bf.append(spliter);
		bf.append(LteScEarfcn_DX);
		bf.append(spliter);
		bf.append(LteScPci_DX);
		bf.append(spliter);
		bf.append(LteScRSRP_LT);
		bf.append(spliter);
		bf.append(LteScRSRQ_LT);
		bf.append(spliter);
		bf.append(LteScEarfcn_LT);
		bf.append(spliter);
		bf.append(LteScPci_LT);
		bf.append(spliter);
		bf.append(moveDirect);
		bf.append(spliter);
		bf.append(location);
		bf.append(spliter);
		bf.append(dist);
		bf.append(spliter);
		bf.append(radius);
		bf.append(spliter);
		bf.append(loctp);
		bf.append(spliter);
		bf.append(ispeed);
		bf.append(spliter);
		bf.append(lable);

		return bf.toString();
	}

	public String createNewSampleToLine()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(cityID);
		bf.append(spliter);
		bf.append(itime);
		bf.append(spliter);
		bf.append(wtimems);
		bf.append(spliter);
		bf.append(ilongitude);
		bf.append(spliter);
		bf.append(ilatitude);
		bf.append(spliter);
		bf.append(ispeed);
		bf.append(spliter);
		bf.append(imode);
		bf.append(spliter);
		bf.append(Func.getEncryptIMSI(IMSI + ""));
		bf.append(spliter);
		bf.append(xdrid);
		bf.append(spliter);
		bf.append(UserLabel);// wifilist
		bf.append(spliter);
		bf.append(Eci / 256);
		bf.append(spliter);
		bf.append(Eci);
		bf.append(spliter);
		bf.append(MmeUeS1apId);
		bf.append(spliter);
		bf.append(imeiTac);
		bf.append(spliter);
		bf.append(LteScRSRP);
		bf.append(spliter);
		bf.append(LteScRSRQ);
		bf.append(spliter);
		bf.append(LteScEarfcn);
		bf.append(spliter);
		bf.append(LteScPci);
		bf.append(spliter);
		bf.append(LteScBSR);
		bf.append(spliter);
		bf.append(LteScRTTD);
		bf.append(spliter);
		bf.append(LteScTadv);
		bf.append(spliter);
		bf.append(LteScAOA);
		bf.append(spliter);
		bf.append(LteScPHR);
		bf.append(spliter);
		bf.append(LteScRIP);
		bf.append(spliter);
		bf.append(LteScSinrUL);
		bf.append(spliter);
		for (int i = 0; i < 3; i++)
		{
			bf.append(nccount[i]);
			bf.append(spliter);
		}
		for (NC_LTE nc_lte : tlte)
		{
			if (nc_lte.LteNcRSRP >= -150 && nc_lte.LteNcRSRP <= -30)
			{
				if (nc_lte.LteNcRSRP >= LteScRSRP - 6)
				{
					if (nc_lte.LteNcEarfcn == LteScEarfcn)
					{
						Overlap++;
					}
					OverlapAll++;
				}
			}
			bf.append(nc_lte.LteNcRSRP);
			bf.append(spliter);
			bf.append(nc_lte.LteNcRSRQ);
			bf.append(spliter);
			bf.append(nc_lte.LteNcEarfcn);
			bf.append(spliter);
			bf.append(nc_lte.LteNcPci);
			bf.append(spliter);
			bf.append("0");
			bf.append(spliter);
		}
		for (int i = 0; i < 3; i++)
		{
			bf.append(tgsm[i].GsmNcellCarrierRSSI);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBcch);
			bf.append(spliter);
			bf.append(tgsm[i].GsmNcellBsic);
			bf.append(spliter);
			bf.append("0");
			bf.append(spliter);
			bf.append("0");
			bf.append(spliter);
		}
		bf.append(Overlap);
		bf.append(spliter);
		bf.append(OverlapAll);
		bf.append(spliter);
		bf.append(mrType);
		bf.append(spliter);
		bf.append(LteScRSRP_DX);
		bf.append(spliter);
		bf.append(LteScRSRQ_DX);
		bf.append(spliter);
		bf.append(LteScEarfcn_DX);
		bf.append(spliter);
		bf.append(LteScPci_DX);
		bf.append(spliter);
		bf.append(LteScRSRP_LT);
		bf.append(spliter);
		bf.append(LteScRSRQ_LT);
		bf.append(spliter);
		bf.append(LteScEarfcn_LT);
		bf.append(spliter);
		bf.append(LteScPci_LT);
		bf.append(spliter);
		bf.append(moveDirect);
		bf.append(spliter);
		bf.append(location);
		bf.append(spliter);
		bf.append(dist);
		bf.append(spliter);
		bf.append(radius);
		bf.append(spliter);
		bf.append(loctp);
		bf.append(spliter);
		bf.append(ispeed);
		bf.append(spliter);
		bf.append(lable);

		return bf.toString();
	}

	// zks add
	public void fillSamData(String[] values)
	{
		int i = 0;
		cityID = Integer.parseInt(values[i++]);
		imeiTac = Integer.parseInt(values[i++]);
		itime = Integer.parseInt(values[i++]);
		wtimems = Short.parseShort(values[i++]);
		bms = Byte.parseByte(values[i++]);
		ilongitude = Integer.parseInt(values[i++]);
		ilatitude = Integer.parseInt(values[i++]);
		ispeed = Integer.parseInt(values[i++]);
		imode = Short.parseShort(values[i++]);
		iLAC = Integer.parseInt(values[i++]);
		iCI = Long.parseLong(values[i++]);
		Eci = Long.parseLong(values[i++]);

		IMSI = Long.parseLong(values[i++]);
		MSISDN = values[i++];

		ENBId = Integer.parseInt(values[i++]);
		UserLabel = values[i++];
		CellId = Long.parseLong(values[i++]);
		Earfcn = Integer.parseInt(values[i++]);

		MmeUeS1apId = Long.parseLong(values[i++]);

		LteScRSRP = Integer.parseInt(values[i++]);
		LteScRSRQ = Integer.parseInt(values[i++]);
		LteScEarfcn = Integer.parseInt(values[i++]);
		LteScPci = Integer.parseInt(values[i++]);
		LteScBSR = Integer.parseInt(values[i++]);

		LteScTadv = Integer.parseInt(values[i++]);
		LteScAOA = Integer.parseInt(values[i++]);

		LteScSinrUL = Integer.parseInt(values[i++]);
		nccount[0] = Short.parseShort(values[i++]);

		ilongitude = Integer.parseInt(values[values.length - 2]);
		ilatitude = Integer.parseInt(values[values.length - 1]);

		// zks 默认赋值的
		location = 7;
		loctp = "lll";
		radius = 80;

	}
	
	public List<EventData> toEventData(){
		List<EventData> eventDatas = new ArrayList<>();
		
		//以qci1判断是否故障事件
		if(MrErrorEventData.ErrorType.isErr(qciData.formatedULQci[0], qciData.formatedDLQci[0])){
			MrErrorEventData errEvent = new MrErrorEventData();
			int i = 0;
			for(;i < qciData.formatedULQci.length; i++){
				errEvent.eventDetial.fvalue[i] = qciData.formatedULQci[i];
			}
			for(int j = 0;j < qciData.formatedDLQci.length; j++,i++){
				errEvent.eventDetial.fvalue[i] = qciData.formatedDLQci[j];
			}
			errEvent.eventDetial.strvalue[0] = MrErrorEventData.ErrorType.fromQCI_1(qciData.formatedULQci[0], qciData.formatedDLQci[0]).getName();
			
			errEvent.iCityID = cityID;
			errEvent.IMSI = IMSI;
			errEvent.iEci = Eci;
			errEvent.iTime = itime;
			errEvent.wTimems = wtimems;
			errEvent.strLoctp = loctp;
			errEvent.strLabel = lable;
			errEvent.iLongitude = ilongitude;
			errEvent.iLatitude = ilatitude;
			
			eventDatas.add(errEvent);
		}
		return eventDatas;
	}

}
