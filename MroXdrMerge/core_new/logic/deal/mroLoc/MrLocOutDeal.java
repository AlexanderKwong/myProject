package logic.deal.mroLoc;

import StructData.SIGNAL_MR_All;
import base.IDataOutputer;
import base.IModel;
import base.deal.impl.OutDeal;
import base.deal.impl.exception.ContinueException;
import jan.util.IWriteLogCallBack;
import jan.util.LOGHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import xdr.locallex.LocItem;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class MrLocOutDeal extends OutDeal<SIGNAL_MR_All> {

    private IDataOutputer dataOutputer;
    private Text curText;
    /**
     * TODO
     *
     * @param dataOutputer 吐出的接口类
     */
    public MrLocOutDeal(IDataOutputer dataOutputer) {
        super(dataOutputer, CompileMark.LocAll);
        curText = new Text();
    }

    @Override
    public  boolean out(SIGNAL_MR_All item) throws Exception {
        if (item.tsc.longitude <= 0)
        {
            throw new ContinueException();
        }
        LocItem loclibItem = new LocItem();
        loclibItem.cityID = item.tsc.cityID;
        loclibItem.itime = item.tsc.beginTime;
        loclibItem.wtimems = (short) item.tsc.beginTimems;
        loclibItem.IMSI = item.tsc.IMSI;
        loclibItem.ilongitude = item.tsc.longitude;
        loclibItem.ilatitude = item.tsc.latitude;
        loclibItem.ibuildid = item.ispeed;
        loclibItem.iheight = item.imode;
        loclibItem.testType = item.testType;
        loclibItem.doorType = item.samState;
        loclibItem.radius = item.radius;
        loclibItem.loctp = item.loctp;
        loclibItem.label = item.lable;
        loclibItem.iAreaType = item.areaType;
        loclibItem.iAreaID = item.areaId;
        loclibItem.locSource = item.locSource;
        loclibItem.LteScRSRP = item.tsc.LteScRSRP;
        loclibItem.LteScSinrUL = item.tsc.LteScSinrUL;
        loclibItem.eci = item.tsc.Eci;
        loclibItem.confidentType = item.ConfidenceType;
        loclibItem.msisdn = item.tsc.Msisdn;
        // yzx add 2017.10.24
        loclibItem.s1apid = item.tsc.MmeUeS1apId;
        curText.set(loclibItem.toString());
        try
        {
            dataOutputer.write("loclib", NullWritable.get(), curText);
        }
        catch (Exception e)
        {
            LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "create mr loclib", e);
            return false;
        }
        return true;
    }

}
