package mdtstat.struct;

import StructData.StaticConfig;

public class MdtMr
{
	public int iCityID;
	public int iTime;
	public int iMdtType;
	public int iMRCnt;
	public int iMRRSRQCnt;
	public float fRSRPValue;
	public float fRSRQValue;
	public int iMRCnt_95;
	public int iMRCnt_100;
	public int iMRCnt_103;
	public int iMRCnt_105;
	public int iMRCnt_110;
	public int iMRCnt_113;
	public int iMRCnt_128;
	public int iRSRQ_14;
	public float fOverlapTotal;
	public int iOverlapMRCnt;
	public float fOverlapTotalAll;
	public int iOverlapMRCntAll;
	public float fRSRPMax = StaticConfig.Int_Abnormal;
	public float fRSRPMin = StaticConfig.Int_Abnormal;
	public float fRSRQMax = StaticConfig.Int_Abnormal;
	public float fRSRQMin = StaticConfig.Int_Abnormal;

}
