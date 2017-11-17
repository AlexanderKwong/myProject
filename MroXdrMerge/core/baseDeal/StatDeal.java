package baseDeal;

import base.IModel;

/**
 * Created by Kwong on 2017/11/10.
 */
public abstract class StatDeal<I extends IModel>  implements IDeal<I, I> {

    /**
     * 统计对象不能修改对象本身
     */
    @Override
    public I deal(I object) throws Exception
    {
        if(!stat(object)){
            throw new ContinueException();
        }
        return object;
    }

    public abstract boolean stat(I object);
}
