package base.deal.impl;

import StructData.Tuple2;
import base.IDataType;
import base.IGroupKey;
import base.IModel;
import model.ModelFactory;

/**
 * Created by Kwong on 2017/11/10.
 */
public class PrepareDeal<K extends IGroupKey> extends MapDeal<Tuple2<K, String>, IModel> {

    protected ModelFactory modelFactory;//注入

    @Override
    public IModel deal(Tuple2<K, String> kv) throws Exception {
        IDataType dataType = kv.first.dataType();
        String value = kv.second;

        return modelFactory.create(dataType, value);

    }

    @Override
    public void flush() {

    }
}
