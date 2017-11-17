package mrstat;

import java.util.ArrayList;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import specialUser.SpecialUserConfig;

public class MrSampleStatDo_4G extends AStatDo
{
	private ArrayList<String> in_sample_high_list;
	private ArrayList<String> in_sample_mid_list;
	private ArrayList<String> in_sample_low_list;
	private ArrayList<String> out_sample_high_list;
	private ArrayList<String> out_sample_mid_list;
	private ArrayList<String> out_sample_low_list;
	private ArrayList<String> vap_mr_sample_list;

	public MrSampleStatDo_4G(TypeResult typeResult)
	{
		super(typeResult);
		// TODO Auto-generated constructor stub
		in_sample_high_list = new ArrayList<String>();
		in_sample_mid_list = new ArrayList<String>();
		in_sample_low_list = new ArrayList<String>();
		out_sample_high_list = new ArrayList<String>();
		out_sample_mid_list = new ArrayList<String>();
		out_sample_low_list = new ArrayList<String>();
		vap_mr_sample_list = new ArrayList<String>();
	}

	@Override
	public int statSub(Object tsam)
	{
		// TODO Auto-generated method stub
		DT_Sample_4G sample = (DT_Sample_4G) tsam;
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) && SpecialUserConfig.GetInstance().ifSpeciUser(sample.IMSI, false))
		{
			vap_mr_sample_list.add(sample.createNewSampleToLine());
		}

		if (sample.Eci > 0)
		{
			if (sample.ConfidenceType == StaticConfig.IH)
			{
				in_sample_high_list.add(sample.createNewSampleToLine());
			}
			else if (sample.ConfidenceType == StaticConfig.IM)
			{
				in_sample_mid_list.add(sample.createNewSampleToLine());
			}
			else if (sample.ConfidenceType == StaticConfig.IL && (!MainModel.GetInstance().getCompile().Assert(CompileMark.NoOutSample)))
			{
				in_sample_low_list.add(sample.createNewSampleToLine());
			}
			else if (sample.ConfidenceType == StaticConfig.OH)
			{
				out_sample_high_list.add(sample.createNewSampleToLine());
			}
			else if (sample.ConfidenceType == StaticConfig.OM)
			{
				out_sample_mid_list.add(sample.createNewSampleToLine());
			}
			else if (sample.ConfidenceType == StaticConfig.OL && (!MainModel.GetInstance().getCompile().Assert(CompileMark.NoOutSample)))
			{
				out_sample_low_list.add(sample.createNewSampleToLine());
			}
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
		for (String item : in_sample_high_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_IN_High_Sample, item);
		}
		for (String item : in_sample_mid_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_IN_Mid_Sample, item);
		}
		for (String item : out_sample_high_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_OUT_High_Sample, item);
		}
		for (String item : out_sample_mid_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_OUT_Mid_Sample, item);
		}
		for (String item : out_sample_low_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_OUT_Low_Sample, item);
		}
		for (String item : in_sample_low_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_IN_Low_Sample, item);
		}
		for (String item : vap_mr_sample_list)
		{
			typeResult.pushData(MroNewTableStat.DataType_Vap_tbMr, item);
		}

		in_sample_high_list.clear();
		in_sample_mid_list.clear();
		in_sample_low_list.clear();
		out_sample_high_list.clear();
		out_sample_mid_list.clear();
		out_sample_low_list.clear();
		vap_mr_sample_list.clear();

		return 0;
	}

}
