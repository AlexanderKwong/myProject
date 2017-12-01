package model;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import base.IDataType;
import base.IModel;
import base.deal.impl.exception.BreakException;
import jan.util.IConfigure;
import mro.lablefill.XdrLable;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Kwong on 2017/11/18.
 */
public class ModelFactory {

    Map<Class<?>, String> modelStringSeperator = new HashMap<>();

    public ModelFactory(IConfigure conf){
        //TODO 通过加载配置，获得对象 与 数据类型 的 关系

        //TODO 通过注解@Model获得所有的model，再通过model类上的每个属性的@FromStrArr(index=$, default=$)为每个Model类生成StrToModelBuilder

        modelStringSeperator.put(SIGNAL_MR_All.class, StaticConfig.DataSliper2);
        modelStringSeperator.put(XdrLable.class, "\t");
    }

    public ModelFactory(){
        modelStringSeperator.put(SIGNAL_MR_All.class, StaticConfig.DataSliper2);
        modelStringSeperator.put(XdrLable.class, "\t");
    }

    public IModel create(IDataType dataType, String value){
        IModel model = null;
        try {
            model = (IModel)dataType.getModelClass().newInstance();
            model.FillData(value.split(modelStringSeperator.get(model.getClass()), -1));
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        if (model == null)
            throw new RuntimeException();
        return model;
    }

}
