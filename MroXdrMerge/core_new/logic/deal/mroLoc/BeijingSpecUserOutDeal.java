package logic.deal.mroLoc;

import StructData.DT_Sample_4G;
import base.IDataOutputer;
import base.deal.impl.OutDeal;
import mroxdrmerge.CompileMark;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import specialUser.SpecialUserConfig;
import xdr.lablefill.ResultHelper;

/**
 * Created by Kwong on 2017/11/27.
 */
public class BeijingSpecUserOutDeal extends OutDeal<DT_Sample_4G> {

    private Text curText = new Text();
    /**
     * TODO
     *
     * @param dataOutputer 吐出的接口类
     */
    public BeijingSpecUserOutDeal(IDataOutputer dataOutputer) {
        super(dataOutputer, CompileMark.BeiJing);
    }

    @Override
    public boolean out(DT_Sample_4G sample) throws Exception {
        if (SpecialUserConfig.GetInstance().ifSpeciUser(sample.IMSI, false))
        {
            curText.set(ResultHelper.getPutLteSample(sample));
            dataOutputer.write("mrvap", NullWritable.get(), curText);
        }
        return true;
    }
}
