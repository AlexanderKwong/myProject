package model;

import base.IDataType;
import base.IModel;
import jan.util.IConfigure;

/**
 * Created by Kwong on 2017/11/18.
 */
public class ModelFactory {

    public ModelFactory(IConfigure conf){
        //TODO 通过加载配置，获得对象 与 数据类型 的 关系

        //TODO 通过注解@Model获得所有的model，再通过model类上的每个属性的@FromStrArr(index=$, default=$)为每个Model类生成StrToModelBuilder
    }

    public IModel create(IDataType dataType, String value){
        return null;
    }
}
