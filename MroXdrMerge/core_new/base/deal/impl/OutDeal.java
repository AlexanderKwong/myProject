package base.deal.impl;

import base.IDataOutputer;
import base.IDeal;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

/**
 * 所有的 吐出 都应该有 一个或多个 相应的编译开关来 控制
 * Created by Kwong on 2017/11/10.
 */
public abstract class OutDeal<I> implements IDeal<I, I> {

    protected IDataOutputer dataOutputer;

    protected boolean effective = false;

    /**
     * TODO
     * @param dataOutputer 吐出的接口类
     */
    public OutDeal(IDataOutputer dataOutputer, int... compileMarks){
        for (int compileMark : compileMarks){
            if (MainModel.GetInstance().getCompile().Assert(compileMark)){
                effective = true;
                this.dataOutputer = dataOutputer;
                break;
            }
        }
    }

    @Override
    public I deal(I o) throws Exception {
        if (effective){
            out(o);
        }
        return o;
    }

    @Override
    public void flush() {

    }

    public abstract boolean out(I o) throws Exception;
}
