package mdtstat.struct;

import StructData.StaticConfig;
import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtOutGrid_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_OutGrid outgrid = new Stat_mdt_OutGrid();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(outgrid.iCityID);
		sbTemp.append("_");
		sbTemp.append(outgrid.iLongitude);
		sbTemp.append("_");
		sbTemp.append(outgrid.iLatitude);
		sbTemp.append("_");
		sbTemp.append(outgrid.ifreq);
		sbTemp.append("_");
		sbTemp.append(outgrid.iMdtType);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(outgrid.iTime));
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
		MdtOutGrid_mergeDo temp = (MdtOutGrid_mergeDo) o;
		outgrid.iMRCnt += temp.outgrid.iMRCnt;
		outgrid.iMRRSRQCnt += temp.outgrid.iMRRSRQCnt;
		outgrid.fRSRPValue += temp.outgrid.fRSRPValue;
		outgrid.fRSRQValue += temp.outgrid.fRSRQValue;
		outgrid.iMRCnt_95 += temp.outgrid.iMRCnt_95;
		outgrid.iMRCnt_100 += temp.outgrid.iMRCnt_100;
		outgrid.iMRCnt_103 += temp.outgrid.iMRCnt_103;
		outgrid.iMRCnt_105 += temp.outgrid.iMRCnt_105;
		outgrid.iMRCnt_110 += temp.outgrid.iMRCnt_110;
		outgrid.iMRCnt_113 += temp.outgrid.iMRCnt_113;
		outgrid.iMRCnt_128 += temp.outgrid.iMRCnt_128;
		outgrid.iRSRQ_14 += temp.outgrid.iRSRQ_14;
		outgrid.fOverlapTotal += temp.outgrid.fOverlapTotal;
		outgrid.iOverlapMRCnt += temp.outgrid.iOverlapMRCnt;
		outgrid.fOverlapTotalAll += temp.outgrid.fOverlapTotalAll;
		outgrid.iOverlapMRCntAll += temp.outgrid.iOverlapMRCntAll;
		if (temp.outgrid.fRSRPMax > outgrid.fRSRPMax)
		{
			outgrid.fRSRPMax = temp.outgrid.fRSRPMax;
		}
		if ((outgrid.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.outgrid.fRSRPMin < outgrid.fRSRPMin && temp.outgrid.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			outgrid.fRSRPMin = temp.outgrid.fRSRPMin;
		}
		if (temp.outgrid.fRSRQMax > outgrid.fRSRQMax)
		{
			outgrid.fRSRQMax = temp.outgrid.fRSRQMax;
		}
		if ((outgrid.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.outgrid.fRSRQMin < outgrid.fRSRQMin && temp.outgrid.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			outgrid.fRSRQMin = temp.outgrid.fRSRQMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		outgrid = Stat_mdt_OutGrid.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return outgrid.roundDayToLine();
	}

}
