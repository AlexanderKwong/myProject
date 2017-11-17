package mrstat;

import java.util.ArrayList;

import StructData.DT_Sample_4G;

public class MrAreaSampleStatDo_4G extends AStatDo
{
	private ArrayList<String> areaSample_list;

	public MrAreaSampleStatDo_4G(TypeResult typeResult)
	{
		super(typeResult);
		areaSample_list = new ArrayList<String>();
	}

	@Override
	public int statSub(Object tsam)
	{
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (sample.Eci > 0 && sample.iAreaType > 0)
		{
			areaSample_list.add(sample.createAreaSampleToLine());
		}
		return 0;
	}

	@Override
	public int outDealingResultSub()
	{
		return 0;
	}

	@Override
	public int outFinalReusltSub()
	{
		for (String item : areaSample_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_AREA_Sample, item);
		}
		areaSample_list.clear();

		return 0;
	}

}
