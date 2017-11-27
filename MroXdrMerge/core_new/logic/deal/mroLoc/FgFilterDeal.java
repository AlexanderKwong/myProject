package logic.deal.mroLoc;

import StructData.DT_Sample_4G;
import base.deal.impl.FilterDeal;
import base.deal.impl.exception.BreakException;

/**
 * Created by Kwong on 2017/11/27.
 */
public class FgFilterDeal extends FilterDeal<DT_Sample_4G>{
    @Override
    public DT_Sample_4G deal(DT_Sample_4G sample) throws Exception {
        if (sample.loctp.contains("fp"))// 过滤掉指纹库定位,常住小区回填的loctp=fp
        {
            sample.ilongitude = 0;
            sample.ilatitude = 0;
            throw new BreakException();
        }
        return sample;
    }

    @Override
    public void flush() {

    }
}
