package mdtstat.struct;

import jan.util.TimeHelper;
import mergestat.IMergeDataDo;

public class MdtImei_mergeDo implements IMergeDataDo
{
	private int dataType = 0;
	public Stat_mdt_imei imei = new Stat_mdt_imei();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(imei.iCityID);
		sbTemp.append("_");
		sbTemp.append(imei.IMEI_TAC);
		sbTemp.append("_");
		sbTemp.append(TimeHelper.getRoundDayTime(imei.iTime));
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
		MdtImei_mergeDo temp = (MdtImei_mergeDo) o;
		imei.im_mdt_total += temp.imei.im_mdt_total;
		imei.im_mdt_loc_80 += temp.imei.im_mdt_loc_80;
		imei.im_mdt_loc_60 += temp.imei.im_mdt_loc_60;
		imei.im_mdt_loc_40 += temp.imei.im_mdt_loc_40;
		imei.im_mdt_loc_20 += temp.imei.im_mdt_loc_20;
		imei.im_mdt_loc_0 += temp.imei.im_mdt_loc_0;
		imei.logged_mdt_total += temp.imei.logged_mdt_total;
		imei.logged_mdt_loc_80 += temp.imei.logged_mdt_loc_80;
		imei.logged_mdt_loc_60 += temp.imei.logged_mdt_loc_60;
		imei.logged_mdt_loc_40 += temp.imei.logged_mdt_loc_40;
		imei.logged_mdt_loc_20 += temp.imei.logged_mdt_loc_20;
		imei.logged_mdt_loc_0 += temp.imei.logged_mdt_loc_0;
		imei.rlf_mdt_total += temp.imei.rlf_mdt_total;
		imei.rlf_mdt_loc_80 += temp.imei.rlf_mdt_loc_80;
		imei.rlf_mdt_loc_60 += temp.imei.rlf_mdt_loc_60;
		imei.rlf_mdt_loc_40 += temp.imei.rlf_mdt_loc_40;
		imei.rlf_mdt_loc_20 += temp.imei.rlf_mdt_loc_20;
		imei.rlf_mdt_loc_0 += temp.imei.rlf_mdt_loc_0;
		imei.rcef_mdt_total += temp.imei.rcef_mdt_total;
		imei.rcef__mdt_loc_80 += temp.imei.rcef__mdt_loc_80;
		imei.rcef__mdt_loc_60 += temp.imei.rcef__mdt_loc_60;
		imei.rcef__mdt_loc_40 += temp.imei.rcef__mdt_loc_40;
		imei.rcef__mdt_loc_20 += temp.imei.rcef__mdt_loc_20;
		imei.rcef__mdt_loc_0 += temp.imei.rcef__mdt_loc_0;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		// TODO Auto-generated method stub
		imei = Stat_mdt_imei.FillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		// TODO Auto-generated method stub
		return imei.roundDayToLine();
	}

}
