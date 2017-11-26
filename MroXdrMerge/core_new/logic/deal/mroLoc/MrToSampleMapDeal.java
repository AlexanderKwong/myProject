package logic.deal.mroLoc;

import ImeiCapbility.ImeiConfig;
import StructData.GridItemOfSize;
import StructData.LteScPlrQciData;
import StructData.NC_LTE;
import StructData.StaticConfig;
import base.deal.impl.MapDeal;
import StructData.DT_Sample_4G;
import StructData.SIGNAL_MR_All;
import mro.lablefillex.MroLocStat;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class MrToSampleMapDeal extends MapDeal<SIGNAL_MR_All, DT_Sample_4G>{

    private int IndoorGridSize = 0;
    private int OutdoorGridSize = 0;
    DT_Sample_4G tsam = new DT_Sample_4G();

    public MrToSampleMapDeal(){
        IndoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getInDoorSize());
        OutdoorGridSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getOutDoorSize());
    }

    @Override
    public DT_Sample_4G deal(SIGNAL_MR_All tTemp) throws Exception {
        tsam.Clear();
        if (tTemp.ispeed > 0)
        {
            tsam.ispeed = tTemp.ispeed;
        }
        else
        {
            tsam.ispeed = -1;
        }
        if (tTemp.imode < 0)
        {
            tsam.imode = -1;
        }
        else
        {
            tsam.imode = tTemp.imode;
        }
        tsam.simuLatitude = tTemp.simuLatitude;
        tsam.simuLongitude = tTemp.simuLongitude;
        tsam.testType = tTemp.testType;
        tsam.samState = tTemp.samState;
        tsam.locSource = tTemp.locSource;
        tsam.cityID = tTemp.tsc.cityID;
        tsam.itime = tTemp.tsc.beginTime;
        tsam.wtimems = (short) (tTemp.tsc.beginTimems);
        tsam.ilongitude = tTemp.tsc.longitude;
        tsam.ilatitude = tTemp.tsc.latitude;
        tsam.IMSI = tTemp.tsc.IMSI;
        tsam.UETac = tTemp.UETac;
        tsam.iLAC = (int) MroLocStat.getValidData(tsam.iLAC, tTemp.tsc.TAC);
        tsam.iCI = (long) MroLocStat.getValidData(tsam.iCI, tTemp.tsc.CellId);
        tsam.Eci = (long) MroLocStat.getValidData(tsam.Eci, tTemp.tsc.Eci);
        tsam.eventType = 0;
        tsam.ENBId = (int) MroLocStat.getValidData(tsam.ENBId, tTemp.tsc.ENBId);
        tsam.UserLabel = tTemp.tsc.UserLabel;
        tsam.wifilist = tTemp.tsc.UserLabel;// mrall 中userlabel中装的wifi信息
        tsam.CellId = (long) MroLocStat.getValidData(tsam.CellId, tTemp.tsc.CellId);
        tsam.Earfcn = tTemp.tsc.Earfcn;
        tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
        tsam.MmeCode = (int) MroLocStat.getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
        tsam.MmeGroupId = (int) MroLocStat.getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
        tsam.MmeUeS1apId = (long) MroLocStat.getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
        tsam.Weight = tTemp.tsc.Weight;
        tsam.LteScRSRP = tTemp.tsc.LteScRSRP;
        tsam.LteScRSRQ = tTemp.tsc.LteScRSRQ;
        tsam.LteScEarfcn = tTemp.tsc.LteScEarfcn;
        tsam.LteScPci = tTemp.tsc.LteScPci;
        tsam.LteScBSR = tTemp.tsc.LteScBSR;
        tsam.LteScRTTD = tTemp.tsc.LteScRTTD;
        tsam.LteScTadv = tTemp.tsc.LteScTadv;
        tsam.LteScAOA = tTemp.tsc.LteScAOA;
        tsam.LteScPHR = tTemp.tsc.LteScPHR;
        tsam.LteScRIP = tTemp.tsc.LteScRIP;
        tsam.LteScSinrUL = tTemp.tsc.LteScSinrUL;
        tsam.LocFillType = 1;

        tsam.testType = tTemp.testType;
        tsam.location = tTemp.location;
        tsam.dist = tTemp.dist;
        tsam.radius = tTemp.radius;
        tsam.loctp = tTemp.loctp;
        tsam.indoor = tTemp.indoor;
        tsam.networktype = tTemp.networktype;
        tsam.lable = tTemp.lable;

        tsam.serviceType = tTemp.serviceType;
        tsam.serviceSubType = tTemp.subServiceType;

        tsam.moveDirect = tTemp.moveDirect;

        tsam.LteScPUSCHPRBNum = tTemp.tsc.LteScPUSCHPRBNum;
        tsam.LteScPDSCHPRBNum = tTemp.tsc.LteScPDSCHPRBNum;
        tsam.imeiTac = tTemp.tsc.imeiTac;
        tsam.fullNetType = ImeiConfig.GetInstance().getValue(tTemp.tsc.imeiTac);
        tsam.eciSwitchList = tTemp.eciSwitchList;
        tsam.ConfidenceType = tTemp.ConfidenceType;
        if (tsam.samState == StaticConfig.ACTTYPE_OUT || tsam.iAreaType > 0)
        {
            tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, OutdoorGridSize);
        }
        else if (tsam.samState == StaticConfig.ACTTYPE_IN)
        {
            tsam.grid = new GridItemOfSize(tsam.cityID, tsam.ilongitude, tsam.ilatitude, IndoorGridSize);
        }
        tsam.MSISDN = tTemp.tsc.Msisdn;

        if (tTemp.tsc.EventType.length() > 0)
        {
            if (tTemp.tsc.EventType.equals("MRO"))
            {
                tsam.flag = "MRO";
            }
            else if (tTemp.tsc.EventType.equals("MDT_IMM"))
            {
                tsam.flag = "MDT_IMM";
            }
            else if (tTemp.tsc.EventType.equals("MDT_LOG"))
            {
                tsam.flag = "MDT_LOG";
            }
            else
            {
                tsam.flag = "MRE";
            }
            tsam.mrType = tTemp.tsc.EventType;
        }
        else
        {
            tsam.flag = "MRO";
            int mrTypeIndex = tTemp.tsc.UserLabel.indexOf(",");
            if (mrTypeIndex >= 0)
            {
                tsam.flag = "MRE";
                tsam.mrType = tTemp.tsc.UserLabel.substring(mrTypeIndex + 1);
            }
        }

        for (int i = 0; i < tsam.nccount.length; i++)
        {
            tsam.nccount[i] = tTemp.nccount[i];
        }

        for (int i = 0; i < tsam.tlte.length; i++)
        {
            tsam.tlte[i] = tTemp.tlte[i];
        }

        for (int i = 0; i < tsam.ttds.length; i++)
        {
            tsam.ttds[i] = tTemp.ttds[i];
        }

        for (int i = 0; i < tsam.tgsm.length; i++)
        {
            tsam.tgsm[i] = tTemp.tgsm[i];
        }

        for (int i = 0; i < tsam.trip.length; i++)
        {
            tsam.trip[i] = tTemp.trip[i];
        }

        tsam.LteScRSRP_DX = tTemp.LteScRSRP_DX;
        tsam.LteScRSRQ_DX = tTemp.LteScRSRQ_DX;
        tsam.LteScEarfcn_DX = tTemp.LteScEarfcn_DX;
        tsam.LteScPci_DX = tTemp.LteScPci_DX;
        tsam.LteScRSRP_LT = tTemp.LteScRSRP_LT;
        tsam.LteScRSRQ_LT = tTemp.LteScRSRQ_LT;
        tsam.LteScEarfcn_LT = tTemp.LteScEarfcn_LT;
        tsam.LteScPci_LT = tTemp.LteScPci_LT;
        tsam.iAreaID = tTemp.areaId;
        tsam.iAreaType = tTemp.areaType;
        // mdt 置信度
        tsam.Confidence = tTemp.Confidence;

        if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng) && tTemp.tgsmall.length > 0)
        {
            for (int i = 0; i < tTemp.tgsmall.length; i++)
            {
                tsam.tgsmall[i] = tTemp.tgsmall[i];
            }
        }

        //20171030 add QCI stat
        tsam.qciData = new LteScPlrQciData(tTemp.tsc.LteScPlrULQci, tTemp.tsc.LteScPlrDLQci);

        calJamType(tsam);
        return tsam;
    }

    @Override
    public void flush() {

    }

    private void calJamType(DT_Sample_4G tsam)
    {
        if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
        {
            for (NC_LTE item : tsam.tlte)
            {
                if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
                {
                    if (tsam.Earfcn == item.LteNcEarfcn)
                    {
                        tsam.sfcnJamCellCount++;
                    }
                    else
                    {
                        tsam.dfcnJamCellCount++;
                    }
                }
            }
        }
    }
}
