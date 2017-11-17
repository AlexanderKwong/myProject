package mdtstat.struct;

import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtCell_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_Cell cell = new Stat_mdt_Cell();
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
		MdtCell_mergeDo temp = (MdtCell_mergeDo) o;
		cell.im_mdt_total += temp.cell.im_mdt_total;
		cell.im_mdt_loc_80 += temp.cell.im_mdt_loc_80;
		cell.im_mdt_loc_60 += temp.cell.im_mdt_loc_60;
		cell.im_mdt_loc_40 += temp.cell.im_mdt_loc_40;
		cell.im_mdt_loc_20 += temp.cell.im_mdt_loc_20;
		cell.im_mdt_loc_0 += temp.cell.im_mdt_loc_0;
		cell.logged_mdt_total += temp.cell.logged_mdt_total;
		cell.logged_mdt_loc_80 += temp.cell.logged_mdt_loc_80;
		cell.logged_mdt_loc_60 += temp.cell.logged_mdt_loc_60;
		cell.logged_mdt_loc_40 += temp.cell.logged_mdt_loc_40;
		cell.logged_mdt_loc_20 += temp.cell.logged_mdt_loc_20;
		cell.logged_mdt_loc_0 += temp.cell.logged_mdt_loc_0;
		cell.rlf_mdt_total += temp.cell.rlf_mdt_total;
		cell.rlf_mdt_loc_80 += temp.cell.rlf_mdt_loc_80;
		cell.rlf_mdt_loc_60 += temp.cell.rlf_mdt_loc_60;
		cell.rlf_mdt_loc_40 += temp.cell.rlf_mdt_loc_40;
		cell.rlf_mdt_loc_20 += temp.cell.rlf_mdt_loc_20;
		cell.rlf_mdt_loc_0 += temp.cell.rlf_mdt_loc_0;
		cell.rcef_mdt_total += temp.cell.rcef_mdt_total;
		cell.rcef_mdt_loc_80 += temp.cell.rcef_mdt_loc_80;
		cell.rcef_mdt_loc_60 += temp.cell.rcef_mdt_loc_60;
		cell.rcef_mdt_loc_40 += temp.cell.rcef_mdt_loc_40;
		cell.rcef_mdt_loc_20 += temp.cell.rcef_mdt_loc_20;
		cell.rcef_mdt_loc_0 += temp.cell.rcef_mdt_loc_0;
		cell.iMRCnt += temp.cell.iMRCnt;
		cell.iMRCnt_Indoor += temp.cell.iMRCnt_Indoor;
		cell.iMRCnt_Outdoor += temp.cell.iMRCnt_Outdoor;
		cell.iMRRSRQCnt += temp.cell.iMRRSRQCnt;
		cell.iMRRSRQCnt_Indoor += temp.cell.iMRRSRQCnt_Indoor;
		cell.iMRRSRQCnt_Outdoor += temp.cell.iMRRSRQCnt_Outdoor;
		cell.fRSRPValue += temp.cell.fRSRPValue;
		cell.fRSRPValue_Indoor += temp.cell.fRSRPValue_Indoor;
		cell.fRSRPValue_Outdoor += temp.cell.fRSRPValue_Outdoor;
		cell.fRSRQValue += temp.cell.fRSRQValue;
		cell.fRSRQValue_Indoor += temp.cell.fRSRQValue_Indoor;
		cell.fRSRQValue_Outdoor += temp.cell.fRSRQValue_Outdoor;
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
		cell.iMRCnt_total_0_70 += temp.cell.iMRCnt_total_0_70;
		cell.iMRCnt_total_70_80 += temp.cell.iMRCnt_total_70_80;
		cell.iMRCnt_total_80_90 += temp.cell.iMRCnt_total_80_90;
		cell.iMRCnt_total_90_95 += temp.cell.iMRCnt_total_90_95;
		cell.iMRCnt_total_100 += temp.cell.iMRCnt_total_100;
		cell.iMRCnt_total_103 += temp.cell.iMRCnt_total_103;
		cell.iMRCnt_total_105 += temp.cell.iMRCnt_total_105;
		cell.iMRCnt_total_110 += temp.cell.iMRCnt_total_110;
		cell.iMRCnt_total_113 += temp.cell.iMRCnt_total_113;
		cell.iRSRQ_Indoor_14 += temp.cell.iRSRQ_Indoor_14;
		cell.iRSRQ_Outdoor_14 += temp.cell.iRSRQ_Outdoor_14;
		cell.iRSRQ_total_14 += temp.cell.iRSRQ_total_14;
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
		cell = Stat_mdt_Cell.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return cell.roundDayToLine();
	}

}
