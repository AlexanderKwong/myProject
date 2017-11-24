package model;

import StructData.*;
import base.IModel;

import java.util.HashSet;
import java.util.Set;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/23
 */
public class SIGNAL_MR_All implements IModel{

    public SIGNAL_MR_SC tsc;
    public short[] nccount;// 4
    public NC_LTE[] tlte;// 6
    public NC_TDS[] ttds;// 6
    public NC_GSM[] tgsm;// 6
    public NC_GSM[] tgsmall; // 6
    public SC_FRAME[] trip;// 10

    // xdr 回填信息
    public int testType;
    public int location;
    public long dist;
    public int radius;
    public String loctp = "";// 指纹库定位fp
    public int indoor;
    public String networktype = "";
    public String lable = "";

    public int serviceType;
    public int subServiceType;

    public int moveDirect;

    // 定位添加字段2017-1-9////////////////////
    public int ispeed;
    public short imode = -1;
    public int simuLongitude;
    public int simuLatitude;
    public String UETac = "";
    // 添加字段5分钟内小区切换信息
    public String eciSwitchList = "";

    // ott/gps/fg新表 20170614
    public int locSource;// 位置来源high/mid/low
    public int samState;// int or out
    // ott/gps/fg 2017.6.19
    public int LteScRSRP_DX; // 电信主服场强
    public int LteScRSRQ_DX; // 电信主服信号质量
    public int LteScEarfcn_DX; // 电信主服频点
    public int LteScPci_DX; // 电信主服PCI
    public int LteScRSRP_LT; // 联通主服场强
    public int LteScRSRQ_LT; // 联通主服信号质量
    public int LteScEarfcn_LT; // 联通主服频点
    public int LteScPci_LT; // 联通主服PCI
    // add 场景统计标识
    public int areaId = -1;
    public int areaType = -1;
    // mdt 置信度
    public int Confidence;
    public int ConfidenceType;// 置信度类型
    // 需要保留异频的场强
    // 将联通和电信的测量结果保存到5个GSM邻区和4个TD邻区里面，共9个频点）
    // 联通：1600,1650,40340
    // 电信：1775,1800,1825,1850,75,100
    private Set<Integer> freqSet = new HashSet<Integer>();
    private int pos = 0;
    public SIGNAL_MR_All()
    {
        Clear();
    }

    public void Clear()
    {
        tsc = new SIGNAL_MR_SC();
        nccount = new short[4];
        tlte = new NC_LTE[6];
        ttds = new NC_TDS[6];
        tgsm = new NC_GSM[6];
        tgsmall = new NC_GSM[6];
        trip = new SC_FRAME[10];

        for (int i = 0; i < nccount.length; i++)
            nccount[i] = 0;

        for (int i = 0; i < 6; i++)
        {
            tlte[i] = new NC_LTE();
            tlte[i].Clear();
            ttds[i] = new NC_TDS();
            ttds[i].Clear();
            tgsm[i] = new NC_GSM();
            tgsm[i].Clear();

            tgsmall[i] = new NC_GSM();
            tgsmall[i].Clear();
        }

        for (int i = 0; i < trip.length; i++)
        {
            trip[i] = new SC_FRAME();
            trip[i].Clear();
        }

        freqSet.clear();
        pos = 0;
    }
}
