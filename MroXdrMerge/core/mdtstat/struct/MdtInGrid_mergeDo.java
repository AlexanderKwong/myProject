package mdtstat.struct;

import StructData.StaticConfig;
import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtInGrid_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_InGrid ingrid = new Stat_mdt_InGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(ingrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(ingrid.iLongitude);
		sbTemp.append("_");
		sbTemp.append(ingrid.iLatitude);
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
		MdtInGrid_mergeDo temp = (MdtInGrid_mergeDo) o;
		ingrid.iMRCnt += temp.ingrid.iMRCnt;
		ingrid.iMRRSRQCnt += temp.ingrid.iMRRSRQCnt;
		ingrid.fRSRPValue += temp.ingrid.fRSRPValue;
		ingrid.fRSRQValue += temp.ingrid.fRSRQValue;
		ingrid.iMRCnt_95 += temp.ingrid.iMRCnt_95;
		ingrid.iMRCnt_100 += temp.ingrid.iMRCnt_100;
		ingrid.iMRCnt_103 += temp.ingrid.iMRCnt_103;
		ingrid.iMRCnt_105 += temp.ingrid.iMRCnt_105;
		ingrid.iMRCnt_110 += temp.ingrid.iMRCnt_110;
		ingrid.iMRCnt_113 += temp.ingrid.iMRCnt_113;
		ingrid.iMRCnt_128 += temp.ingrid.iMRCnt_128;
		ingrid.iRSRQ_14 += temp.ingrid.iRSRQ_14;
		ingrid.fOverlapTotal += temp.ingrid.fOverlapTotal;
		ingrid.iOverlapMRCnt += temp.ingrid.iOverlapMRCnt;
		ingrid.fOverlapTotalAll += temp.ingrid.fOverlapTotalAll;
		ingrid.iOverlapMRCntAll += temp.ingrid.iOverlapMRCntAll;
		if (temp.ingrid.fRSRPMax > ingrid.fRSRPMax)
		{
			ingrid.fRSRPMax = temp.ingrid.fRSRPMax;
		}
		if ((ingrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.ingrid.fRSRPMin < ingrid.fRSRPMin && temp.ingrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			ingrid.fRSRPMin = temp.ingrid.fRSRPMin;
		}
		if (temp.ingrid.fRSRQMax > ingrid.fRSRQMax)
		{
			ingrid.fRSRQMax = temp.ingrid.fRSRQMax;
		}
		if ((ingrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.ingrid.fRSRQMin < ingrid.fRSRQMin && temp.ingrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			ingrid.fRSRQMin = temp.ingrid.fRSRQMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		ingrid = Stat_mdt_InGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return ingrid.roundDayToLine();
	}
}
