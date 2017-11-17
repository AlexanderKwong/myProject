package mdtstat.struct;

import StructData.StaticConfig;
import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtOutCellGrid_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_Out_CellGrid outcellGrid = new Stat_mdt_Out_CellGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outcellGrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iECI);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iLongitude);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iLatitude);
		sbTemp.append("_");
		sbTemp.append(outcellGrid.iMdtType);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(outcellGrid.iTime));
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
		MdtOutCellGrid_mergeDo temp = (MdtOutCellGrid_mergeDo) o;
		outcellGrid.iMRCnt += temp.outcellGrid.iMRCnt;
		outcellGrid.iMRRSRQCnt += temp.outcellGrid.iMRRSRQCnt;
		outcellGrid.fRSRPValue += temp.outcellGrid.fRSRPValue;
		outcellGrid.fRSRQValue += temp.outcellGrid.fRSRQValue;
		outcellGrid.iMRCnt_95 += temp.outcellGrid.iMRCnt_95;
		outcellGrid.iMRCnt_100 += temp.outcellGrid.iMRCnt_100;
		outcellGrid.iMRCnt_103 += temp.outcellGrid.iMRCnt_103;
		outcellGrid.iMRCnt_105 += temp.outcellGrid.iMRCnt_105;
		outcellGrid.iMRCnt_110 += temp.outcellGrid.iMRCnt_110;
		outcellGrid.iMRCnt_113 += temp.outcellGrid.iMRCnt_113;
		outcellGrid.iMRCnt_128 += temp.outcellGrid.iMRCnt_128;
		outcellGrid.iRSRQ_14 += temp.outcellGrid.iRSRQ_14;
		outcellGrid.fOverlapTotal += temp.outcellGrid.fOverlapTotal;
		outcellGrid.iOverlapMRCnt += temp.outcellGrid.iOverlapMRCnt;
		outcellGrid.fOverlapTotalAll += temp.outcellGrid.fOverlapTotalAll;
		outcellGrid.iOverlapMRCntAll += temp.outcellGrid.iOverlapMRCntAll;
		if (temp.outcellGrid.fRSRPMax > outcellGrid.fRSRPMax)
		{
			outcellGrid.fRSRPMax = temp.outcellGrid.fRSRPMax;
		}
		if ((outcellGrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.outcellGrid.fRSRPMin < outcellGrid.fRSRPMin && temp.outcellGrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			outcellGrid.fRSRPMin = temp.outcellGrid.fRSRPMin;
		}
		if (temp.outcellGrid.fRSRQMax > outcellGrid.fRSRQMax)
		{
			outcellGrid.fRSRQMax = temp.outcellGrid.fRSRQMax;
		}
		if ((outcellGrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.outcellGrid.fRSRQMin < outcellGrid.fRSRQMin && temp.outcellGrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			outcellGrid.fRSRQMin = temp.outcellGrid.fRSRQMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		outcellGrid = Stat_mdt_Out_CellGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		return outcellGrid.roundDayToLine();
	}

}
