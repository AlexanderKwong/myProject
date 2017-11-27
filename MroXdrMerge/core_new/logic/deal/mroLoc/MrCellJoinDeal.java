package logic.deal.mroLoc;

import StructData.StaticConfig;
import base.IModel;
import base.deal.impl.exception.BreakException;
import base.deal.impl.exception.ContinueException;
import jan.util.GisFunction;
import logic.deal.mroLoc.join.CellMng;
import base.deal.impl.JoinDeal;
import StructData.SIGNAL_MR_All;
import mro.lablefillex.WifiFixed;
import util.Func;
import util.MrLocation;

/**
 * TODO 必须在 {@link MrXdrJoinDeal}之后, 定位之后才能得到 室内室外
 * Created by Kwong on 2017/11/21.
 */
public class MrCellJoinDeal extends JoinDeal<SIGNAL_MR_All>{

    private long eci;
    private CellMng cellMng;

    public void init(long eci){
        if (this.eci != eci){
            this.eci = eci;
            try{
                cellMng = (CellMng)new CellMng.Builder(eci).create();
            }catch (Exception e){
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public SIGNAL_MR_All deal(IModel o) throws Exception {
        if(o instanceof SIGNAL_MR_All){
            SIGNAL_MR_All mrAll = (SIGNAL_MR_All)o;
            if (mrAll == null || mrAll.tsc == null || mrAll.tsc.MmeUeS1apId <= 0 || mrAll.tsc.Eci <= 0 || mrAll.tsc.beginTime <= 0)
                throw new ContinueException();

            if(eci == 0 || cellMng == null){
                init(mrAll.tsc.Eci);
            }

            // 附上地市id
            mrAll.tsc.cityID = cellMng.cellInfo.cityid;

            getInOrOut(mrAll);

            filterByDist(mrAll);

            return mrAll;
        }
//        return o;
        throw new BreakException();
    }

    private void getInOrOut(SIGNAL_MR_All mrAll)
    {
            if (mrAll.tsc.longitude <= 0 || mrAll.tsc.IMSI <= 0)// 没有关联上xdr
            {
                return;
            }
            if (mrAll.testType != StaticConfig.TestType_CQT)
            {
                mrAll.samState = StaticConfig.ACTTYPE_OUT;
                return;
            }
            try
            {
                int buildId = cellMng.cellBuild.getBuildId(mrAll.tsc.longitude, mrAll.tsc.latitude);
                if (buildId != 0)
                {
                    mrAll.samState = StaticConfig.ACTTYPE_IN;
                    mrAll.ispeed = buildId;
                    if (cellMng.cellBuildWifi.getCellBuildWifiMap().size() > 0)
                    {
                        mrAll.imode = (short) WifiFixed.returnLevel(cellMng.cellBuildWifi, mrAll.tsc.UserLabel, mrAll.ispeed);
                    }
                    else
                    {
                        mrAll.imode = -1;
                    }
                }
                else
                {
                    mrAll.samState = StaticConfig.ACTTYPE_OUT;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
    }

    private void filterByDist(SIGNAL_MR_All data){
        int maxRadius = 6000;
        // 如果采样点过远就需要筛除
        int dist = -1;
        if (cellMng.cellInfo != null)
        {
            if (data.tsc.longitude > 0 && data.tsc.latitude > 0 && cellMng.cellInfo.ilongitude > 0 && cellMng.cellInfo.ilatitude > 0)
            {
                dist = (int) GisFunction.GetDistance(data.tsc.longitude, data.tsc.latitude, cellMng.cellInfo.ilongitude, cellMng.cellInfo.ilatitude);
            }
        }
        data.dist = dist;
        if (dist > maxRadius)
        {
            data.dist = -1;
            data.tsc.longitude = 0;
            data.tsc.latitude = 0;
            data.testType = StaticConfig.TestType_OTHER;
        }

        // 基于Ta进行筛
        if (data.tsc.LteScTadv >= 15 && data.tsc.LteScTadv < 1282)
        {
            double taDist = MrLocation.calcDist(data.tsc.LteScTadv, data.tsc.LteScRTTD);
            if (dist > taDist * 1.2)
            {
                data.dist = -1;
                data.tsc.longitude = 0;
                data.tsc.latitude = 0;
                data.testType = StaticConfig.TestType_OTHER;
            }
        }
        data.ConfidenceType = Func.getSampleConfidentType(data.locSource, data.samState, data.testType);
    }
    
    @Override
    public void flush() {
        eci = 0;
        cellMng = null;
    }
}
