package base;

import base.IGroupKey;

/**
 * Created by Kwong on 2017/11/10.
 */
public interface IDeal<I, O> {

//    void init(IGroupKey key);

    O deal(I o) throws Exception;

    void flush();
}
