package baseDeal;

import StructData.Tuple2;
import base.IGroupKey;
import base.IModel;

/**
 * Created by Kwong on 2017/11/10.
 */
public abstract class PrepareDeal<K extends IGroupKey, V extends IModel> implements IDeal<Tuple2<K, String>, V> {

    @Override
    public V deal(Tuple2<K, String> o) throws Exception {
        return null;
    }

    @Override
    public void flush() {

    }
}
