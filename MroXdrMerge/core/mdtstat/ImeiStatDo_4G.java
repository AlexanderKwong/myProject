package mdtstat;

import java.util.HashMap;
import java.util.Map;

import StructData.DT_Sample_4G;
import mdtstat.struct.Stat_mdt_imei;
import mrstat.AStatDo;
import mrstat.MrStatUtil;
import mrstat.TypeResult;

public class ImeiStatDo_4G extends AStatDo
{
	private Map<String, Stat_mdt_imei> imeiDataMap;
	private int sourceType;

	public ImeiStatDo_4G(TypeResult typeResult, int sourceType)
	{
		super(typeResult);
		this.sourceType = sourceType;
		imeiDataMap = new HashMap<String, Stat_mdt_imei>();
	}

	@Override
	public int statSub(Object tsam)
	{
		// TODO Auto-generated method stub
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (sample.imeiTac > 0)
		{
			String key = sample.cityID + "_" + sample.imeiTac + "_" + Util.RoundTimeForHour(sample.itime);
			Stat_mdt_imei imei = imeiDataMap.get(key);
			if (imei == null)
			{
				imei = new Stat_mdt_imei();
				imei.doFirstSample(sample);
				imeiDataMap.put(key, imei);
			}
			imei.doSample(sample);
		}

		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		// TODO Auto-generated method stub
		for (Stat_mdt_imei item : imeiDataMap.values())
		{
			typeResult.pushData(MdtNewTableStat.DataType_TB_MDT_IMEI, item.toLine());
		}
		imeiDataMap.clear();
		return 0;
	}

}
