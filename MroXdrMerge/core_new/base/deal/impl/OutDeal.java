package base.deal.impl;

import base.IDataOutputer;
import base.IDeal;

/**
 * Created by Kwong on 2017/11/10.
 */
public abstract class OutDeal<I, O> implements IDeal<I, O> {

    protected IDataOutputer dataOutputer;

    /**
     * TODO
     * @param dataOutputer 吐出的接口类
     */
    public OutDeal(IDataOutputer dataOutputer){

    }
}
