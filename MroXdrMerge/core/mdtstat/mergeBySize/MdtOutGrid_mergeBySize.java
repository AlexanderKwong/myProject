package mdtstat.mergeBySize;

import util.Func;
import jan.util.TimeHelper;
import mdtstat.struct.MdtOutGrid_mergeDo;
import mroxdrmerge.MainModel;

public class MdtOutGrid_mergeBySize extends MdtOutGrid_mergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeOut =Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeOut());
	
	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outgrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeOut, outgrid.iLongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeOut, outgrid.iLatitude));
		sbTemp.append("_");
		sbTemp.append(outgrid.ifreq);
		sbTemp.append("_");
		sbTemp.append(outgrid.iMdtType);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(outgrid.iTime));
		return sbTemp.toString();
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(outgrid.iCityID);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeOut, outgrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeOut, outgrid.iLatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeOut, outgrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeOut, outgrid.iLatitude));
		bf.append(spliter);
		bf.append(outgrid.ifreq);
		bf.append(spliter);
		bf.append(outgrid.iMdtType);
		bf.append(spliter);
		bf.append(outgrid.iTime);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt);
		bf.append(spliter);
		bf.append(outgrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(outgrid.fRSRPValue);
		bf.append(spliter);
		bf.append(outgrid.fRSRQValue);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(outgrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(outgrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(outgrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(outgrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(outgrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(outgrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(outgrid.fRSRPMax);
		bf.append(spliter);
		bf.append(outgrid.fRSRPMin);
		bf.append(spliter);
		bf.append(outgrid.fRSRQMax);
		bf.append(spliter);
		bf.append(outgrid.fRSRQMin);
		return bf.toString();
	}
	
}
