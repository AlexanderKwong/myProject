package logic.deal.mroLoc;

import StructData.StaticConfig;
import base.IDataOutputer;
import base.deal.impl.OutDeal;
import base.deal.impl.exception.ContinueException;

import StructData.DT_Sample_4G;
import mro.evt.EventData;
import mro.evt.MrErrorEventData;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import specialUser.SpecialUserConfig;
import xdr.lablefill.ResultHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class SampleOutDeal extends OutDeal<DT_Sample_4G> {
    private Text curText = new Text();
    private StringBuilder tmSb = new StringBuilder();

    private List<OutDeal<DT_Sample_4G>> outDeals;
    /**
     * TODO
     *
     * @param dataOutputer 吐出的接口类
     */
    public SampleOutDeal(IDataOutputer dataOutputer) {
        super(dataOutputer, CompileMark.OutAllSample, CompileMark.MroDetail, CompileMark.Debug);
        effective = true;

        outDeals = new ArrayList<>();

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.OutAllSample)){
            outDeals.add(new OutDeal<DT_Sample_4G>(dataOutputer, CompileMark.OutAllSample) {
                @Override
                public boolean out(DT_Sample_4G sample) throws Exception {
                    curText.set(ResultHelper.getPutLteSample(sample));
                    dataOutputer.write("mrosample", NullWritable.get(), curText);
                    return true;
                }
            });
        }

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.MroDetail))
        {
            outDeals.add(new OutDeal<DT_Sample_4G>(dataOutputer, CompileMark.MroDetail) {
            @Override
            public boolean out(DT_Sample_4G sample) throws Exception {
                List<EventData> evtDatas = sample.toEventData();
                StringBuffer bf = new StringBuffer();
                for(EventData evtData : evtDatas)
                {
                    if(evtData instanceof MrErrorEventData){
                        evtData.toString(bf);
                        curText.set(bf.toString());
                        dataOutputer.write("mroErrEvt", NullWritable.get(), curText);
                        bf.delete(0, bf.length());
                    }
                }
                return true;
            }
        });
        }

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug)){
            outDeals.add(new OutDeal<DT_Sample_4G>(dataOutputer, CompileMark.MroDetail) {

                @Override
                public boolean out(DT_Sample_4G sample) throws Exception {
                    // 吐出关联的中间结果
                    tmSb.delete(0, tmSb.length());
                    tmSb.append(sample.Eci + "_" + sample.MmeUeS1apId + "_" + sample.itime);
                    tmSb.append("\t");
                    tmSb.append(sample.Earfcn);
                    tmSb.append("_");
                    tmSb.append(sample.LteScPci);
                    tmSb.append("_");
                    tmSb.append(sample.LteScRSRP);
                    tmSb.append("_");
                    tmSb.append(sample.IMSI);
                    tmSb.append("_");
                    tmSb.append(sample.ilongitude);
                    tmSb.append("_");
                    tmSb.append(sample.ilatitude);

                    curText.set(tmSb.toString());
                    dataOutputer.write("mroMore", NullWritable.get(), curText);
                    return true;
                }
            });
        }
    }

    @Override
    public boolean out(DT_Sample_4G sample) throws Exception {

        for(OutDeal<DT_Sample_4G> deal : outDeals){
            deal.deal(sample);
        }

        if (sample.testType == StaticConfig.TestType_DT)
        {
            if (sample.ilongitude > 0)
            {
                curText.set(ResultHelper.getPutLteSample(sample));

                dataOutputer.write("sampledt", NullWritable.get(), curText);
            }
        }
        else if (sample.testType == StaticConfig.TestType_DT_EX || sample.testType == StaticConfig.TestType_CPE)
        {
            if (sample.ilongitude > 0)
            {
                curText.set(ResultHelper.getPutLteSample(sample));
                dataOutputer.write("sampledtex", NullWritable.get(), curText);
            }
        }
        else if (sample.testType == StaticConfig.TestType_CQT)
        {
            curText.set(ResultHelper.getPutLteSample(sample));
            dataOutputer.write("samplecqt", NullWritable.get(), curText);
        }

        return true;
    }

    @Override
    public void flush() {

    }
}
