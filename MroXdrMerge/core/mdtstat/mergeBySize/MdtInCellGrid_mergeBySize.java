package mdtstat.mergeBySize;

import jan.util.TimeHelper;
import mdtstat.struct.MdtInCellGrid_mergeDo;
import mroxdrmerge.MainModel;
import util.Func;

public class MdtInCellGrid_mergeBySize extends MdtInCellGrid_mergeDo
{
	private StringBuffer sbTemp = new StringBuffer();
	public static final String spliter = "\t";
	public int roundSizeIn = Integer.parseInt(MainModel.GetInstance().getAppConfig().getRoundSizeIn());

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(incellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLongtitude(roundSizeIn, incellGrid.iLongitude));
		sbTemp.append("_");
		sbTemp.append(Func.getRoundLatitude(roundSizeIn, incellGrid.iLatitude));
		sbTemp.append("_");
		sbTemp.append(incellGrid.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iHeight);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iMdtType);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(incellGrid.iTime));
		return sbTemp.toString();
	}

	@Override
	public String getData()
	{
		StringBuffer bf = new StringBuffer();
		bf.append(incellGrid.iCityID);
		bf.append(spliter);
		bf.append(incellGrid.iBuildingID);
		bf.append(spliter);
		bf.append(incellGrid.iHeight);
		bf.append(spliter);
		bf.append(Func.getRoundLongtitude(roundSizeIn, incellGrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundTLLatitude(roundSizeIn, incellGrid.iLatitude));
		bf.append(spliter);
		bf.append(Func.getRoundBRLongtitude(roundSizeIn, incellGrid.iLongitude));
		bf.append(spliter);
		bf.append(Func.getRoundLatitude(roundSizeIn, incellGrid.iLatitude));
		bf.append(spliter);
		bf.append(incellGrid.iECI);
		bf.append(spliter);
		bf.append(incellGrid.iMdtType);
		bf.append(spliter);
		bf.append(incellGrid.iTime);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt);
		bf.append(spliter);
		bf.append(incellGrid.iMRRSRQCnt);
		bf.append(spliter);
		bf.append(incellGrid.fRSRPValue);
		bf.append(spliter);
		bf.append(incellGrid.fRSRQValue);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_95);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_100);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_103);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_105);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_110);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_113);
		bf.append(spliter);
		bf.append(incellGrid.iMRCnt_128);
		bf.append(spliter);
		bf.append(incellGrid.iRSRQ_14);
		bf.append(spliter);
		bf.append(incellGrid.fOverlapTotal);
		bf.append(spliter);
		bf.append(incellGrid.iOverlapMRCnt);
		bf.append(spliter);
		bf.append(incellGrid.fOverlapTotalAll);
		bf.append(spliter);
		bf.append(incellGrid.iOverlapMRCntAll);
		bf.append(spliter);
		bf.append(incellGrid.fRSRPMax);
		bf.append(spliter);
		bf.append(incellGrid.fRSRPMin);
		bf.append(spliter);
		bf.append(incellGrid.fRSRQMax);
		bf.append(spliter);
		bf.append(incellGrid.fRSRQMin);
		return bf.toString();
	}

}
