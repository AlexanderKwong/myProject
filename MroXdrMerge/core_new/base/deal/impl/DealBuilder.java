package base.deal.impl;

import base.IDeal;
import base.deal.impl.exception.BreakException;
import base.deal.impl.exception.ContinueException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kwong on 2017/11/10.
 */
public class DealBuilder<T, K> implements IDeal<T, K> {

    private List<IDeal> dealMng = new ArrayList<>();

    public DealBuilder(){}

    public DealBuilder(IDeal<T, K> deal){
        dealMng.add(deal);
    }

    /**
     * 根据PECS原则
     * @param deal
     * @param <J>
     * @return
     */
    public <J> DealBuilder<T, J> append(IDeal<? extends K, J> deal) {
        dealMng.add(deal);
        DealBuilder<T, J> builder = new DealBuilder<>();
        builder.dealMng = this.dealMng;
        return builder;
    }

    public IDeal<T, K> create() {
        return this;
    }

    public K deal(T o) throws Exception{
        Object result = o;
        for (IDeal deal : dealMng) {
            try {
                result = deal.deal(result);
            } catch (ContinueException e) {
                continue;
            } catch (BreakException e) {
                break;
            }
        }
        return null;
    }

    @Override
    public void flush() {
        for (IDeal deal : dealMng) {
            try {
                deal.flush();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
