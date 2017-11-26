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

import java.util.List;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class SampleOutDeal extends OutDeal<DT_Sample_4G, DT_Sample_4G> {
    Text curText = new Text();
    private StringBuilder tmSb = new StringBuilder();
    /**
     * TODO
     *
     * @param dataOutputer 吐出的接口类
     */
    public SampleOutDeal(IDataOutputer dataOutputer) {
        super(dataOutputer);
    }

    @Override
    public DT_Sample_4G deal(DT_Sample_4G o) throws Exception {
        DT_Sample_4G sample = (DT_Sample_4G)o;
        if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing) && SpecialUserConfig.GetInstance().ifSpeciUser(o.IMSI, false))
        {
            curText.set(ResultHelper.getPutLteSample(sample));
            dataOutputer.write("mrvap", NullWritable.get(), curText);
        }

        if (sample.loctp.contains("fp"))// 过滤掉指纹库定位,常住小区回填的loctp=fp
        {
            sample.ilongitude = 0;
            sample.ilatitude = 0;
            throw new ContinueException();
        }

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.OutAllSample))
        {
            curText.set(ResultHelper.getPutLteSample(sample));
            dataOutputer.write("mrosample", NullWritable.get(), curText);
        }

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.MroDetail))
        {
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
        if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
        {
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
        }
        return o;
    }

    @Override
    public void flush() {

    }
}
