package logic.deal.mroLoc;

import StructData.StaticConfig;
import base.IDataOutputer;
import base.IModel;
import base.deal.impl.OutDeal;
import mro.lablefill.XdrLable;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import util.Func;
import xdr.locallex.LocItem;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class XdrLocOutDeal extends OutDeal<IModel, IModel>{

    Text curText;
    StringBuilder sb;

    public XdrLocOutDeal(IDataOutputer dataOutputer){
        super(dataOutputer);
        curText = new Text();
        sb = new StringBuilder();
    }

    @Override
    public IModel deal(IModel o) throws Exception {
        if(o instanceof XdrLable){
            XdrLable xdrLable = (XdrLable)o;
            if ((xdrLable.longitudeGL > 0 || xdrLable.ilongtude > 0) /*&& (xdrLable.itime / TimeSpan * TimeSpan == key.getTimeSpan())*/
                    && MainModel.GetInstance().getCompile().Assert(CompileMark.LocAll))
            {
//                curText.set(outPutLocLib(xdrLable));
//                mosMng.write("xdrloclib", NullWritable.get(), curText);// 吐出xdr位置库

//                sb.append(outPutLocLib(xdrLable)).append("\n"); // 不放内存
                curText.set(outPutLocLib(xdrLable));
                dataOutputer.write("xdrloclib", NullWritable.get(), curText);// 吐出xdr位置库
            }
        }
        return o;
    }

    @Override
    public void flush() {

    }

    private String outPutLocLib(XdrLable xdrlocation)
    {
        LocItem loclibItem = new LocItem();
        loclibItem.cityID = xdrlocation.cityID;
        loclibItem.itime = xdrlocation.itime;
        loclibItem.wtimems = 0;
        loclibItem.IMSI = xdrlocation.imsi;
        loclibItem.ilongitude = xdrlocation.longitudeGL;
        loclibItem.ilatitude = xdrlocation.latitudeGL;
        loclibItem.testType = xdrlocation.testType;
        if (loclibItem.testType != StaticConfig.TestType_CQT)
        {
            loclibItem.doorType = StaticConfig.ACTTYPE_OUT;
        }
        else
        {
//            loclibItem.ibuildid = cellBuild.getBuildId(xdrlocation.longitudeGL, xdrlocation.latitudeGL);
//            loclibItem.iheight = WifiFixed.returnLevel(cellBuildWifi, xdrlocation.wifiName, loclibItem.ibuildid);
            loclibItem.ibuildid = xdrlocation.ibuildid;
            loclibItem.iheight = xdrlocation.iheight;
            if (loclibItem.ibuildid > 0)
            {
                loclibItem.doorType = StaticConfig.ACTTYPE_IN;
            }
            else
            {
                loclibItem.doorType = StaticConfig.ACTTYPE_OUT;
            }
        }
        loclibItem.radius = xdrlocation.radius;
        loclibItem.loctp = xdrlocation.loctp;
        loclibItem.label = xdrlocation.lable;
        loclibItem.iAreaType = xdrlocation.areaType;
        loclibItem.iAreaID = xdrlocation.areaId;
        loclibItem.locSource = Func.getLocSource(xdrlocation.loctp);
        loclibItem.LteScRSRP = 0;
        loclibItem.LteScSinrUL = 0;
        loclibItem.eci = xdrlocation.eci;
        loclibItem.confidentType = Func.getSampleConfidentType(xdrlocation.loctp, loclibItem.doorType, xdrlocation.testType);
        // yzx add 2017.10.24
        loclibItem.s1apid = xdrlocation.s1apid;
        loclibItem.msisdn = xdrlocation.msisdn;
        return loclibItem.toString();
    }
}
