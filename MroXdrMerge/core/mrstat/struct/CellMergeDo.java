package mrstat.struct;

import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class CellMergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_Cell cell = new Stat_Cell();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(cell.iCityID);
		sbTemp.append("_");
		sbTemp.append(cell.iECI);
		sbTemp.append("_");
		sbTemp.append(cell.ifreq);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(cell.iTime));
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
		CellMergeDo temp = (CellMergeDo) o;
		cell.iMRCnt += temp.cell.iMRCnt;
		cell.iMRCnt_Indoor += temp.cell.iMRCnt_Indoor;
		cell.iMRCnt_Outdoor += temp.cell.iMRCnt_Outdoor;
		cell.iMRRSRQCnt += temp.cell.iMRRSRQCnt;
		cell.iMRRSRQCnt_Indoor += temp.cell.iMRRSRQCnt_Indoor;
		cell.iMRRSRQCnt_Outdoor += temp.cell.iMRRSRQCnt_Outdoor;
		cell.iMRSINRCnt += temp.cell.iMRSINRCnt;
		cell.iMRSINRCnt_Indoor += temp.cell.iMRSINRCnt_Indoor;
		cell.iMRSINRCnt_Outdoor += temp.cell.iMRSINRCnt_Outdoor;
		cell.fRSRPValue += temp.cell.fRSRPValue;
		cell.fRSRPValue_Indoor += temp.cell.fRSRPValue_Indoor;
		cell.fRSRPValue_Outdoor += temp.cell.fRSRPValue_Outdoor;
		cell.fRSRQValue += temp.cell.fRSRQValue;
		cell.fRSRQValue_Indoor += temp.cell.fRSRQValue_Indoor;
		cell.fRSRQValue_Outdoor += temp.cell.fRSRQValue_Outdoor;
		cell.fSINRValue += temp.cell.fSINRValue;
		cell.fSINRValue_Indoor += temp.cell.fSINRValue_Indoor;
		cell.fSINRValue_Outdoor += temp.cell.fSINRValue_Outdoor;
		cell.iMRCnt_Indoor_0_70 += temp.cell.iMRCnt_Indoor_0_70;
		cell.iMRCnt_Indoor_70_80 += temp.cell.iMRCnt_Indoor_70_80;
		cell.iMRCnt_Indoor_80_90 += temp.cell.iMRCnt_Indoor_80_90;
		cell.iMRCnt_Indoor_90_95 += temp.cell.iMRCnt_Indoor_90_95;
		cell.iMRCnt_Indoor_100 += temp.cell.iMRCnt_Indoor_100;
		cell.iMRCnt_Indoor_103 += temp.cell.iMRCnt_Indoor_103;
		cell.iMRCnt_Indoor_105 += temp.cell.iMRCnt_Indoor_105;
		cell.iMRCnt_Indoor_110 += temp.cell.iMRCnt_Indoor_110;
		cell.iMRCnt_Indoor_113 += temp.cell.iMRCnt_Indoor_113;
		cell.iMRCnt_Outdoor_0_70 += temp.cell.iMRCnt_Outdoor_0_70;
		cell.iMRCnt_Outdoor_70_80 += temp.cell.iMRCnt_Outdoor_70_80;
		cell.iMRCnt_Outdoor_80_90 += temp.cell.iMRCnt_Outdoor_80_90;
		cell.iMRCnt_Outdoor_90_95 += temp.cell.iMRCnt_Outdoor_90_95;
		cell.iMRCnt_Outdoor_100 += temp.cell.iMRCnt_Outdoor_100;
		cell.iMRCnt_Outdoor_103 += temp.cell.iMRCnt_Outdoor_103;
		cell.iMRCnt_Outdoor_105 += temp.cell.iMRCnt_Outdoor_105;
		cell.iMRCnt_Outdoor_110 += temp.cell.iMRCnt_Outdoor_110;
		cell.iMRCnt_Outdoor_113 += temp.cell.iMRCnt_Outdoor_113;
		cell.iIndoorRSRP100_SINR0 += temp.cell.iIndoorRSRP100_SINR0;
		cell.iIndoorRSRP105_SINR0 += temp.cell.iIndoorRSRP105_SINR0;
		cell.iIndoorRSRP110_SINR3 += temp.cell.iIndoorRSRP110_SINR3;
		cell.iIndoorRSRP110_SINR0 += temp.cell.iIndoorRSRP110_SINR0;
		cell.iOutdoorRSRP100_SINR0 += temp.cell.iOutdoorRSRP100_SINR0;
		cell.iOutdoorRSRP105_SINR0 += temp.cell.iOutdoorRSRP105_SINR0;
		cell.iOutdoorRSRP110_SINR3 += temp.cell.iOutdoorRSRP110_SINR3;
		cell.iOutdoorRSRP110_SINR0 += temp.cell.iOutdoorRSRP110_SINR0;
		cell.iSINR_Indoor_0 += temp.cell.iSINR_Indoor_0;
		cell.iRSRQ_Indoor_14 += temp.cell.iRSRQ_Indoor_14;
		cell.iSINR_Outdoor_0 += temp.cell.iSINR_Outdoor_0;
		cell.iRSRQ_Outdoor_14 += temp.cell.iRSRQ_Outdoor_14;
		cell.fOverlapTotal += temp.cell.fOverlapTotal;
		cell.iOverlapMRCnt += temp.cell.iOverlapMRCnt;
		cell.fOverlapTotalAll += temp.cell.fOverlapTotalAll;
		cell.iOverlapMRCntAll += temp.cell.iOverlapMRCntAll;

		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		try
		{
			cell = Stat_Cell.FillData(vals, 0);
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return cell.roundDayToLine();
	}

}
