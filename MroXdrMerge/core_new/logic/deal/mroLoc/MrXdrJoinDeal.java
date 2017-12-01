package logic.deal.mroLoc;

import base.IModel;
import base.deal.impl.JoinDeal;
import base.deal.impl.exception.BreakException;
import logic.deal.mroLoc.join.XdrLableMng;
import StructData.SIGNAL_MR_All;
import mro.lablefill.XdrLable;

/**
 * Created by Kwong on 2017/11/21.
 */
public class MrXdrJoinDeal extends JoinDeal<IModel>{

    private XdrLableMng xdrLableMng;
    private XdrLableMng.Builder xdrLableMngBuilder;

    public MrXdrJoinDeal(){
        xdrLableMngBuilder = new XdrLableMng.Builder();
    }

    @Override
    public IModel deal(IModel o) throws Exception {
        if (o instanceof XdrLable)
        {
            xdrLableMngBuilder.append((XdrLable)o);
        }
        else if (o instanceof SIGNAL_MR_All)
        {
            if (xdrLableMng == null)
            {
                xdrLableMng = (XdrLableMng)xdrLableMngBuilder.create();
            }
            SIGNAL_MR_All mr = (SIGNAL_MR_All)o;
            xdrLableMng.dealMroData(mr);// ott定位

            return mr;
        }
        throw new BreakException();
    }

    @Override
    public void flush() {
        xdrLableMngBuilder = new XdrLableMng.Builder();;
        xdrLableMng = null;
    }



}
