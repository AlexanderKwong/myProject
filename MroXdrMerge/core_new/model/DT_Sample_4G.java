package model;

import StructData.*;
import base.IModel;

/**
 * Created by Kwong on 2017/11/21.
 */
public class DT_Sample_4G implements IModel{

    public int cityID;
    public int fileID;
    public int imeiTac;// 保存imei
    public int itime;
    public short wtimems;
    public byte bms;
    public int ilongitude;
    public int ilatitude;
    public int ispeed;// 楼宇id
    public short imode;// 楼层
    public int iLAC;
    public long iCI;
    public long Eci;
    public long IMSI;
    public String MSISDN;
    public String UETac;
    public String UEBrand;
    public String UEType;
    public int serviceType;
    public int serviceSubType;
    public String urlDomain;
    public long IPDataUL;
    public long IPDataDL;
    public int duration;
    public double IPThroughputUL;
    public double IPThroughputDL;
    public int IPPacketUL;
    public int IPPacketDL;
    public int TCPReTranPacketUL;
    public int TCPReTranPacketDL;
    public int sessionRequest;
    public int sessionResult;
    public int eventType;
    public int userType;
    public String eNBName;
    public int eNBLongitude;
    public int eNBLatitude;
    public int eNBDistance;
    public String flag;
    public int ENBId;
    public String UserLabel;
    public long CellId;
    public int Earfcn;
    public int SubFrameNbr;
    public int MmeCode;
    public int MmeGroupId;
    public long MmeUeS1apId;
    public int Weight;
    public int LteScRSRP;
    public int LteScRSRQ;
    public int LteScEarfcn;
    public int LteScPci;
    public int LteScBSR;
    public int LteScRTTD;
    public int LteScTadv;
    public int LteScAOA;
    public int LteScPHR;
    public int LteScRIP;
    public int LteScSinrUL;
    public short[] nccount;
    public NC_LTE[] tlte;
    public NC_TDS[] ttds;// imeiTac
    public NC_GSM[] tgsm;
    public NC_GSM[] tgsmall; // neimeng
    public SC_FRAME[] trip;
    public int LocFillType;// 0是正常值，1是经纬度回填

    public int testType;
    public int location;
    public long dist;
    public int radius;
    public String loctp;
    public int indoor;

    public String networktype;
    public String lable;// static,low,unknow,high

    public int simuLongitude;
    public int simuLatitude;

    public int moveDirect;
    public String mrType;

    public int dfcnJamCellCount;// 异频干扰小区个数
    public int sfcnJamCellCount;// 同频干扰小区个数

    public int LteScPUSCHPRBNum;
    public int LteScPDSCHPRBNum;
    public int LteSceNBRxTxTimeDiff;
    // new add 前后5分钟小区切换信息
    public String eciSwitchList;
    // -------------------------------------------mr新统计分YD/LT/DX 2017.6.14

    // 新添加字段表示经纬度来源和运动状态
    public String wifilist;
    public String xdrid = "";// 关联XDRID
    public int LteScRSRP_DX; // 电信主服场强
    public int LteScRSRQ_DX; // 电信主服信号质量
    public int LteScEarfcn_DX; // 电信主服频点
    public int LteScPci_DX; // 电信主服PCI
    public int LteScRSRP_LT; // 联通主服场强
    public int LteScRSRQ_LT; // 联通主服信号质量
    public int LteScEarfcn_LT; // 联通主服频点
    public int LteScPci_LT; // 联通主服PCI

    public int locSource;// 位置精度 high /mid /low
    public int samState;// int or out

    // 重叠覆盖
    public int Overlap = 1;
    public int OverlapAll = 1;
    // 场景统计
    public int iAreaType = -1;
    public int iAreaID = -1;
    // mdt 置信度
    public int Confidence;

    public int ConfidenceType;// 置信度类型

    public GridItemOfSize grid;
    public int fullNetType;// 全网通类型
}
