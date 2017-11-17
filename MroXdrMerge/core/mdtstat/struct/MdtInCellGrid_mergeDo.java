package mdtstat.struct;

import StructData.StaticConfig;
import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtInCellGrid_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_In_CellGrid incellGrid = new Stat_mdt_In_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(incellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iLongitude);
		sbTemp.append("_");
		sbTemp.append(incellGrid.iLatitude);
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
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		// TODO Auto-generated method stub
		MdtInCellGrid_mergeDo temp = (MdtInCellGrid_mergeDo) o;
		incellGrid.iMRCnt += temp.incellGrid.iMRCnt;
		incellGrid.iMRRSRQCnt += temp.incellGrid.iMRRSRQCnt;
		incellGrid.fRSRPValue += temp.incellGrid.fRSRPValue;
		incellGrid.fRSRQValue += temp.incellGrid.fRSRQValue;
		incellGrid.iMRCnt_95 += temp.incellGrid.iMRCnt_95;
		incellGrid.iMRCnt_100 += temp.incellGrid.iMRCnt_100;
		incellGrid.iMRCnt_103 += temp.incellGrid.iMRCnt_103;
		incellGrid.iMRCnt_105 += temp.incellGrid.iMRCnt_105;
		incellGrid.iMRCnt_110 += temp.incellGrid.iMRCnt_110;
		incellGrid.iMRCnt_113 += temp.incellGrid.iMRCnt_113;
		incellGrid.iMRCnt_128 += temp.incellGrid.iMRCnt_128;
		incellGrid.iRSRQ_14 += temp.incellGrid.iRSRQ_14;
		incellGrid.fOverlapTotal += temp.incellGrid.fOverlapTotal;
		incellGrid.iOverlapMRCnt += temp.incellGrid.iOverlapMRCnt;
		incellGrid.fOverlapTotalAll += temp.incellGrid.fOverlapTotalAll;
		incellGrid.iOverlapMRCntAll += temp.incellGrid.iOverlapMRCntAll;
		if (temp.incellGrid.fRSRPMax > incellGrid.fRSRPMax)
		{
			incellGrid.fRSRPMax = temp.incellGrid.fRSRPMax;
		}
		if ((incellGrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.incellGrid.fRSRPMin < incellGrid.fRSRPMin && temp.incellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			incellGrid.fRSRPMin = temp.incellGrid.fRSRPMin;
		}
		if (temp.incellGrid.fRSRQMax > incellGrid.fRSRQMax)
		{
			incellGrid.fRSRQMax = temp.incellGrid.fRSRQMax;
		}
		if ((incellGrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.incellGrid.fRSRQMin < incellGrid.fRSRQMin && temp.incellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			incellGrid.fRSRQMin = temp.incellGrid.fRSRQMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		incellGrid = Stat_mdt_In_CellGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return incellGrid.roundDayToLine();
	}

}
