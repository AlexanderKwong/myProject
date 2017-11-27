package logic.deal.mroLoc;

import base.IDataOutputer;
import base.deal.impl.StatDeal;
import mdtstat.UserMdtStat;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import mrstat.UserMrStat;
import StructData.DT_Sample_4G;
import mrstat.TypeInfo;
import mrstat.TypeInfoMng;
import mrstat.TypeResult;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.Map;

/**
 * 新表计算
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class FgOttStatDeal extends StatDeal<DT_Sample_4G>{

    // mdt
    private UserMdtStat userMdtStat;

    private UserMrStat userStat;

    private TypeResult mdtTypeResult, typeResult;

    private IDataOutputer dataOutputer;
    
    private Text curText;

    private boolean effective;

    public FgOttStatDeal(IDataOutputer dataOutputer, TypeInfoMng typeInfoMng, TypeInfoMng mdtTypeInfoMng){
        this.dataOutputer = dataOutputer;
        // mdt new table
        mdtTypeResult = new TypeResult(mdtTypeInfoMng);
        userMdtStat = new UserMdtStat(mdtTypeResult);
        // fgottstat
        typeResult = new TypeResult(typeInfoMng);
        userStat = new UserMrStat(typeResult);
        
        curText = new Text();

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.fgOttStat)){
            effective = true;
        }
    }

    @Override
    public void flush() {
        if (effective)
        {
            // mro新表吐出
            userStat.outResult();
            for (Map.Entry<TypeInfo, StringBuffer> entry : typeResult.getMapEntry())
            {
                curText.set(entry.getValue().toString());
                try
                {
                    dataOutputer.write(entry.getKey().getOutPutName(), NullWritable.get(), curText);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
            typeResult.cleanMap();
            // mdt新表吐出
            userMdtStat.outResult();
            for (Map.Entry<TypeInfo, StringBuffer> entry : mdtTypeResult.getMapEntry())
            {
                curText.set(entry.getValue().toString());
                try
                {
                    dataOutputer.write(entry.getKey().getOutPutName(), NullWritable.get(), curText);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
            mdtTypeResult.cleanMap();
        }
    }

    @Override
    public boolean stat(DT_Sample_4G sample) {
        if (effective)
        {
            if (sample.mrType.equals("MDT_IMM") || sample.mrType.equals("MDT_LOG"))
            {
                userMdtStat.dealSample(sample);
//                return;
            }
            else
            {
                userStat.dealSample(sample);
            }
        }
        return true;
    }
}
