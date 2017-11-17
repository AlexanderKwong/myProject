package mdtstat.mergeBySize;

import util.Func;
import jan.util.TimeHelper;
import mdtstat.struct.MdtOutCellGrid_mergeDo;
import mroxdrmerge.MainModel;

public class MdtOutCellGrid_mergeBySize extends MdtOutCellGrid_mergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeOut =Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeOut());
	
	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outcellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeOut, outcellGrid.iLongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeOut, outcellGrid.iLatitude));
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iMdtType);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(outcellGrid.iTime));
		return sbTemp.toString();
	}
	
	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(outcellGrid.iCityID);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeOut, outcellGrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeOut, outcellGrid.iLatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeOut, outcellGrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeOut, outcellGrid.iLatitude));
		bf.append(spliter);
		bf.append(outcellGrid.iECI);
		bf.append(spliter);
		bf.append(outcellGrid.iMdtType);
		bf.append(spliter);
		bf.append(outcellGrid.iTime);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt);
		bf.append(spliter);
		bf.append(outcellGrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRPValue);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRQValue);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(outcellGrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(outcellGrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(outcellGrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(outcellGrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(outcellGrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(outcellGrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRPMax);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRPMin);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRQMax);
		bf.append(spliter);
		bf.append(outcellGrid.fRSRQMin);
		return bf.toString();
	}
}
