package mdtstat.struct;

import StructData.StaticConfig;
import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtBuildCell_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_BuildCell buildcell = new Stat_mdt_BuildCell();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(buildcell.iCityID);
		sbTemp.append("_");
		sbTemp.append(buildcell.iECI);
		sbTemp.append("_");
		sbTemp.append(buildcell.iBuildingID);
		sbTemp.append("_");
		sbTemp.append(buildcell.iHeight);
		sbTemp.append("_");
		sbTemp.append(buildcell.ifreq);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(buildcell.iTime));
		sbTemp.append("_");
		sbTemp.append(buildcell.iMdtType);
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
		MdtBuildCell_mergeDo temp = (MdtBuildCell_mergeDo) o;
		buildcell.iMRCnt += temp.buildcell.iMRCnt;
		buildcell.iMRRSRQCnt += temp.buildcell.iMRRSRQCnt;
		buildcell.fRSRPValue += temp.buildcell.fRSRPValue;
		buildcell.fRSRQValue += temp.buildcell.fRSRQValue;
		buildcell.iMRCnt_95 += temp.buildcell.iMRCnt_95;
		buildcell.iMRCnt_100 += temp.buildcell.iMRCnt_100;
		buildcell.iMRCnt_103 += temp.buildcell.iMRCnt_103;
		buildcell.iMRCnt_105 += temp.buildcell.iMRCnt_105;
		buildcell.iMRCnt_110 += temp.buildcell.iMRCnt_110;
		buildcell.iMRCnt_113 += temp.buildcell.iMRCnt_113;
		buildcell.iMRCnt_128 += temp.buildcell.iMRCnt_128;
		buildcell.iRSRQ_14 += temp.buildcell.iRSRQ_14;
		buildcell.fOverlapTotal += temp.buildcell.fOverlapTotal;
		buildcell.iOverlapMRCnt += temp.buildcell.iOverlapMRCnt;
		buildcell.fOverlapTotalAll += temp.buildcell.fOverlapTotalAll;
		buildcell.iOverlapMRCntAll += temp.buildcell.iOverlapMRCntAll;
		if (temp.buildcell.fRSRPMax > buildcell.fRSRPMax)
		{
			buildcell.fRSRPMax = temp.buildcell.fRSRPMax;
		}
		if ((buildcell.fRSRPMin == StaticConfig.Int_Abnormal) || (temp.buildcell.fRSRPMin < buildcell.fRSRPMin && temp.buildcell.fRSRPMin != StaticConfig.Int_Abnormal))
		{
			buildcell.fRSRPMin = temp.buildcell.fRSRPMin;
		}
		if (temp.buildcell.fRSRQMax > buildcell.fRSRQMax)
		{
			buildcell.fRSRQMax = temp.buildcell.fRSRQMax;
		}
		if ((buildcell.fRSRQMin == StaticConfig.Int_Abnormal) || (temp.buildcell.fRSRQMin < buildcell.fRSRQMin && temp.buildcell.fRSRQMin != StaticConfig.Int_Abnormal))
		{
			buildcell.fRSRQMin = temp.buildcell.fRSRQMin;
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		buildcell = Stat_mdt_BuildCell.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return buildcell.roundDayToLine();
	}

}
