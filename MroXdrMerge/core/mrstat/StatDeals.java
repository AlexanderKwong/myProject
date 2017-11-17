package mrstat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import StructData.DT_Sample_23G;
import StructData.DT_Sample_4G;
import StructData.StaticConfig;


/**
 * @Description 数据处理
 * @author kwong
 * @date 20170919
 */
//TODO ①个人认为这里将具体的 统计类抽象出去(如OutGridStatDo_4G)，再分YD/LT/DX (OutGridStatDo_4G_YD)具体的处理类会更好，通过增加类来减少循环判断(单一职责原则)，减少状态传递，也方便管理。(PS:这里真正执行统计功能的是AStatDo的子类，而不是DayDataDeal_4G
//TODO ②typeResult参数仅在输出时用到，不必在构造函数中传入，增加类的耦合度
//TODO ③DayDataDeal_4G应该有一个上层的接口，而成员dealList的泛型应该是这个接口，那么到时候有变更的 如加入WeekDataDeal/MonthDataDeal/SeasonDataDeal就可以直接加入到成员dealList中，而不影响上下游结构
//TODO ④在mrstat包中应该分包(package)存放不同层次的接口/类，清晰结构，方便管理
public class StatDeals
{
	private List<DayDataDeal_4G> dealList;
	
	/**
	 * 构造函数，会注入一些默认统计策略
	 * @param typeResult
	 */
	public StatDeals(TypeResult typeResult)
	{
		//默认添加 YD,LT,DX
		this(typeResult, 
				//YD
				new DayDataDeal_4G(new CompositeStatDo(Arrays.asList(new IStatDo[]{
						// 栅格统计
						new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						// 小区栅格统计
						new OutGridCellStatDo_4G(typeResult),
						new InGridCellStatDo_4G(typeResult),
						// 楼宇统计 、楼宇小区统计、小区统计
						new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						new BuildCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						new CellStatDO_4G(typeResult, StaticConfig.SOURCE_YD),
						// 覆盖采样点
						new MrSampleStatDo_4G(typeResult),
						// 区域相关统计
						new MrAreaSampleStatDo_4G(typeResult),
						new AreaGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						new AreaCellGridStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						new AreaCellStatDo_4G(typeResult, StaticConfig.SOURCE_YD),
						new AreaStatDo_4G(typeResult, StaticConfig.SOURCE_YD)
				}))),
				//LT
				new DayDataDeal_4G(new CompositeStatDo(Arrays.asList(new IStatDo[]{
						new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT),
						new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT),
						new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_LT),
						new CellStatDO_4G(typeResult, StaticConfig.SOURCE_LT),
						new AreaGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT),
//						new AreaCellGridStatDo_4G(typeResult, StaticConfig.SOURCE_LT),
						new AreaCellStatDo_4G(typeResult, StaticConfig.SOURCE_LT),
						new AreaStatDo_4G(typeResult, StaticConfig.SOURCE_LT)
				}))),
				//DX
				new DayDataDeal_4G(new CompositeStatDo(Arrays.asList(new IStatDo[]{
						new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_DX),
						new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_DX),
						new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_DX),
						new CellStatDO_4G(typeResult, StaticConfig.SOURCE_DX),
						new AreaGridStatDo_4G(typeResult, StaticConfig.SOURCE_DX),
//						new AreaCellGridStatDo_4G(typeResult, StaticConfig.SOURCE_DX),
						new AreaCellStatDo_4G(typeResult, StaticConfig.SOURCE_DX),
						new AreaStatDo_4G(typeResult, StaticConfig.SOURCE_DX)
				}))),
				//20170920 add YDLT
				new DayDataDeal_4G(new CompositeStatDo(Arrays.asList(new IStatDo[]{
						new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT),
						new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT),
						new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT),
						new CellStatDO_4G(typeResult, StaticConfig.SOURCE_YDLT),
						new AreaGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT),
//						new AreaCellGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT),
						new AreaCellStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT),
						new AreaStatDo_4G(typeResult, StaticConfig.SOURCE_YDLT)
				}))){
					@Override
					public void dealSample(DT_Sample_4G sample){
						//fullNetType说明： 0=LT/DX都不支持；1=DX支持；2=LT支持；3=LT/DX都支持
						if((sample.fullNetType & 2) > 0){
							super.dealSample(sample);
						}
					}
				},
				//20170920 add YDDX
				new DayDataDeal_4G(new CompositeStatDo(Arrays.asList(new IStatDo[]{
						new OutGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX),
						new InGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX),
						new BuildStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX),
						new CellStatDO_4G(typeResult, StaticConfig.SOURCE_YDDX),
						new AreaGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX),
//						new AreaCellGridStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX),
						new AreaCellStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX),
						new AreaStatDo_4G(typeResult, StaticConfig.SOURCE_YDDX)
				}))){
					@Override
					public void dealSample(DT_Sample_4G sample){
						//fullNetType说明： 0=LT/DX都不支持；1=DX支持；2=LT支持；3=LT/DX都支持
						if((sample.fullNetType & 1) > 0){
							super.dealSample(sample);
						}
					}
				}
			);
		
	}
	/**
	 * 构造函数，调用此构造函数需要注入统计策略，把策略暴露给调用者
	 * @param typeResult
	 * @param dataDeals 
	 */
	public StatDeals(TypeResult typeResult, DayDataDeal_4G...dataDeals )
	{
		dealList = new ArrayList<>(Arrays.asList(dataDeals));
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
		for(DayDataDeal_4G deal : dealList){
			deal.dealSample(sample);
		}
	}
	
	public void dealSample(DT_Sample_23G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
	}

	public int outResult()
	{
		for(DayDataDeal_4G deal : dealList){
			deal.outResult();
		}

		return 0;
	}

}
