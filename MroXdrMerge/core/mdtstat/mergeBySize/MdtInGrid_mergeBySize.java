package mdtstat.mergeBySize;

import util.Func;
import jan.util.TimeHelper;
import mdtstat.struct.MdtInGrid_mergeDo;
import mroxdrmerge.MainModel;

public class MdtInGrid_mergeBySize extends MdtInGrid_mergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeIn = Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeIn());
	
	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(ingrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeIn, ingrid.iLongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeIn, ingrid.iLatitude));
		sbTemp.append("_");
		sbTemp.append(ingrid.ifreq);
		sbTemp.append("_");
		sbTemp.append(ingrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(ingrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(ingrid.iMdtType);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(ingrid.iTime));
		return sbTemp.toString();
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(ingrid.iCityID);
		bf.append(spliter);
		bf.append(ingrid.iBuildingID);
		bf.append(spliter);
		bf.append(ingrid.iHeight);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeIn, ingrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeIn, ingrid.iLatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeIn, ingrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeIn, ingrid.iLatitude));
		bf.append(spliter);
		bf.append(ingrid.ifreq);
		bf.append(spliter);
		bf.append(ingrid.iMdtType);
		bf.append(spliter);
		bf.append(ingrid.iTime);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt);
		bf.append(spliter);
		bf.append(ingrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(ingrid.fRSRPValue);
		bf.append(spliter);
		bf.append(ingrid.fRSRQValue);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(ingrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(ingrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(ingrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(ingrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(ingrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(ingrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(ingrid.fRSRPMax);
		bf.append(spliter);
		bf.append(ingrid.fRSRPMin);
		bf.append(spliter);
		bf.append(ingrid.fRSRQMax);
		bf.append(spliter);
		bf.append(ingrid.fRSRQMin);
		return bf.toString();
	}
	
}
