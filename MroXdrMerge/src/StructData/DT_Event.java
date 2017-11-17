package StructData;

import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.StringUtil;

public class DT_Event
{
	public Long imsi;

	public int cityID;
	public int Procedure_Status;
	public int projectID;
	public int SampleID;
	public int itime;
	public short wtimems;
	public byte bms;
	public int eventID;
	public int ilongitude;
	public int ilatitude;
	public int cqtposid;
	public int iLAC;
	public short wRAC;
	public long iCI;
	public int iTargetLAC;
	public short wTargetRAC;
	public int iTargetCI;
	public long ivalue1;
	public long ivalue2;
	public long ivalue3;
	public long ivalue4;
	public long ivalue5;
	public long ivalue6;
	public long ivalue7;
	public long ivalue8;
	public long ivalue9;
	public long ivalue10;
	public int LocFillType;

	// 新增
	public int testType;
	public int location;
	public long dist;
	public int radius;
	public String loctp;
	public int indoor;

	public String networktype;
	public String lable;

	public int moveDirect;

	public void Clear()
	{
		imsi = StaticConfig.Long_Abnormal;
		cityID = StaticConfig.Int_Abnormal;
		Procedure_Status = StaticConfig.Int_Abnormal;
		projectID = StaticConfig.Int_Abnormal;
		SampleID = StaticConfig.Int_Abnormal;
		itime = StaticConfig.Int_Abnormal;
		wtimems = StaticConfig.Short_Abnormal;
		bms = 0;
		eventID = StaticConfig.Int_Abnormal;
		ilongitude = StaticConfig.Int_Abnormal;
		ilatitude = StaticConfig.Int_Abnormal;
		cqtposid = StaticConfig.Int_Abnormal;
		iLAC = StaticConfig.Int_Abnormal;
		wRAC = StaticConfig.Short_Abnormal;
		iCI = StaticConfig.Int_Abnormal;
		iTargetLAC = StaticConfig.Int_Abnormal;
		wTargetRAC = StaticConfig.Short_Abnormal;
		iTargetCI = StaticConfig.Int_Abnormal;
		ivalue1 = StaticConfig.Int_Abnormal;
		ivalue2 = StaticConfig.Int_Abnormal;
		ivalue3 = StaticConfig.Int_Abnormal;
		ivalue4 = StaticConfig.Int_Abnormal;
		ivalue5 = StaticConfig.Int_Abnormal;
		ivalue6 = StaticConfig.Int_Abnormal;
		ivalue7 = StaticConfig.Int_Abnormal;
		ivalue8 = StaticConfig.Int_Abnormal;
		ivalue9 = StaticConfig.Int_Abnormal;
		ivalue10 = StaticConfig.Int_Abnormal;
		LocFillType = 0;
		testType = -1;
		location = -1;
		dist = -1;
		radius = -1;
		loctp = "unknown";
		indoor = -1;
		networktype = "unknown";
		lable = "unknown";
		moveDirect = -1;
	}

	public String getEncryptIMSI()
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.EncryptUser))
		{
			return StringUtil.EncryptStringToLong(imsi + "") + "";
		}
		return imsi + "";
	}

}
