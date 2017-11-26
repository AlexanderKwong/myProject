package logic.deal.mroLoc.join;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import base.deal.impl.JoinDeal;
import mro.lablefill.XdrLable;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import noSatisUser.GL_BaseInfo;
import util.Func;

import java.util.*;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class XdrLableMng implements JoinDeal.ModelMng<XdrLable> {
    @Override
    public List<XdrLable> get(JoinDeal.JoinCondition joinCondition) {
        return null;
    }

    public static class Builder implements JoinDeal.ModelMngBuilder<XdrLable> {

        XdrLableMng xdrLableMng ;

        public Builder(){
            xdrLableMng = new XdrLableMng();
        }

        @Override
        public JoinDeal.ModelMngBuilder<XdrLable> append(XdrLable xdrLable) {
            xdrLableMng.addXdrLocItem(xdrLable);
            return this;
        }

        @Override
        public JoinDeal.ModelMng<XdrLable> create() {
            xdrLableMng.init();
            return xdrLableMng;
        }
    }

    /*****************************
     * the following are old codes
     */
    private long eci;
    // 基于s1apid的xdr列表
    private Map<Long, S1apIDMngExtend> s1apIDMap = new HashMap<Long, S1apIDMngExtend>();
    // 基于imsi的xdr列表
    private Map<Long, ImsiS1apIDMngExtend> imsiS1apIDMap = new HashMap<Long, ImsiS1apIDMngExtend>();

    private XdrLableMng()
    {

    }

    public String getSize()
    {
        return "s1apIDMap.size=" + s1apIDMap.size() + " imsiS1apIDMap.size=" + imsiS1apIDMap.size();
    }

    private void addXdrLocItem(XdrLable xdrLable)
    {
        {
            if (xdrLable.s1apid > 0)
            {
                S1apIDMngExtend mng = s1apIDMap.get(xdrLable.s1apid);
                if (mng == null)
                {
                    mng = new S1apIDMngExtend();
                    s1apIDMap.put(xdrLable.s1apid, mng);
                }
                mng.addXdrLocItem(xdrLable);
            }

        }

        {
            ImsiS1apIDMngExtend mng = imsiS1apIDMap.get(xdrLable.imsi);
            if (mng == null)
            {
                mng = new ImsiS1apIDMngExtend();
                imsiS1apIDMap.put(xdrLable.imsi, mng);
            }
            mng.addXdrLocItem(xdrLable);
        }
    }

    private void init()
    {
        for (S1apIDMngExtend item : s1apIDMap.values())
        {
            item.init();
        }

        for (ImsiS1apIDMngExtend item : imsiS1apIDMap.values())
        {
            item.init();
        }
    }

    public long getEci()
    {
        return eci;
    }

    /**
     * 关联imsi
     *
     * @param baseInfo
     * @return
     */
    public boolean dealGLBaseInfo(GL_BaseInfo baseInfo)
    {
        S1apIDMngExtend s1apIDMng = s1apIDMap.get(baseInfo.mmeUes1apid);
        if (s1apIDMng == null)
        {
            return false;
        }
        XdrLable locItem = s1apIDMng.getXdrLoc(baseInfo.times);
        if (locItem != null)
        {
            baseInfo.imsi = locItem.imsi;
        }
        else
        {
            return false;
        }
        return true;
    }

    public boolean dealMroData(SIGNAL_MR_All mroItem)
    {
        if (mroItem.tsc.EventType.equals("MDT_IMM") || mroItem.tsc.EventType.equals("MDT_LOG"))
        {
            mroItem.testType = StaticConfig.TestType_DT_EX;
        }
        {
            // 关联imsi
            S1apIDMngExtend s1apIDMng = s1apIDMap.get(mroItem.tsc.MmeUeS1apId);
            if (s1apIDMng == null)
            {
                return false;
            }

            XdrLable locItem = s1apIDMng.getXdrLoc(mroItem.tsc.beginTime);
            if (locItem != null)
            {
                mroItem.tsc.IMSI = locItem.imsi;
                mroItem.tsc.imeiTac = locItem.imeiTac;
                mroItem.eciSwitchList = locItem.eciSwitchList;
            }
            else
            {
                return false;
            }
        }

        {
            // 关联lable
            ImsiS1apIDMngExtend imsiMng = imsiS1apIDMap.get(mroItem.tsc.IMSI);
            if (imsiMng == null)
            {
                return false;
            }

            XdrLable lableItem = imsiMng.getXdrLocEx(mroItem.tsc.beginTime);
            if (lableItem != null)
            {
                mroItem.testType = lableItem.testTypeGL;
                mroItem.location = lableItem.locationGL;
                mroItem.dist = lableItem.distGL;
                mroItem.radius = lableItem.radiusGL;
                mroItem.loctp = lableItem.loctpGL;
                mroItem.indoor = lableItem.indoorGL;
                mroItem.lable = lableItem.lableGL;
                // mdt数据不回填经纬度 locSource 判断方式不同
                if (mroItem.tsc.EventType.equals("MDT_IMM") || mroItem.tsc.EventType.equals("MDT_LOG"))
                {
                    return true;
                }

                mroItem.locSource = Func.getLocSource(mroItem.loctp);
                if (mroItem.locSource == 0)
                {
                    mroItem.locSource = Func.getLocSource(mroItem.location);
                }

                mroItem.serviceType = lableItem.serviceType;
                mroItem.subServiceType = lableItem.subServiceType;

                mroItem.moveDirect = lableItem.moveDirect;

                mroItem.tsc.UserLabel = lableItem.wifiName;
                if (!MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))// 北京采样点不要手机号位置库要手机号
                {
                    mroItem.tsc.Msisdn = lableItem.msisdn;
                }
                if (lableItem.testTypeGL == StaticConfig.TestType_DT || lableItem.testTypeGL == StaticConfig.TestType_DT_EX)
                {
                    // 贵州数据要求60秒都可以回填
                    if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuiZhou))
                    {
                        if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 60)
                        {
                            mroItem.tsc.longitude = lableItem.longitudeGL;
                            mroItem.tsc.latitude = lableItem.latitudeGL;
                        }
                    }
                    else
                    {
                        if (Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 10)
                        {
                            mroItem.tsc.longitude = lableItem.longitudeGL;
                            mroItem.tsc.latitude = lableItem.latitudeGL;
                        }
                    }
                }
                else if (lableItem.testTypeGL == StaticConfig.TestType_CQT)
                {
                    mroItem.tsc.longitude = lableItem.longitudeGL;
                    mroItem.tsc.latitude = lableItem.latitudeGL;
                }
                else if (lableItem.testTypeGL == StaticConfig.TestType_HiRail)
                {
                    mroItem.areaId = lableItem.areaId;
                    mroItem.areaType = lableItem.areaType;
                    mroItem.tsc.latitude = lableItem.latitudeGL;
                    mroItem.tsc.longitude = lableItem.longitudeGL;
                }
            }
        }
        return true;
    }
    private class S1apIDMngExtend
    {
        private List<XdrLable> xdrLableList;
        private int timeExpend = 5 * 60;
        private int TimeSpan = 600;

        public S1apIDMngExtend()
        {
            xdrLableList = new ArrayList<XdrLable>();
        }

        public void addXdrLocItem(XdrLable lableItem)
        {
            xdrLableList.add(lableItem);
        }

        public void init()
        {
            Collections.sort(xdrLableList, new Comparator<XdrLable>()
            {
                public int compare(XdrLable a, XdrLable b)
                {
                    return a.itime - b.itime;
                }
            });
        }

        public XdrLable getXdrLoc(int tmTime)
        {
            int curTimeSpan = timeExpend;
            XdrLable curItem = null;
            Set<Long> imsiSet = new HashSet<Long>();
            for (XdrLable item : xdrLableList)
            {
                // 过早的loc,跳过
                if (item.itime + timeExpend < tmTime)
                    continue;

                // 过晚的loc，跳过
                if (tmTime + timeExpend < item.itime)
                    break;

                if (item.imsi > 0)
                {
                    imsiSet.add(item.imsi);
                }
                // 前后5分钟如果同一个usapid有两个及以上的imsi 丢弃
                if (imsiSet.size() > 1)
                {
                    return null;
                }
                int tmTimeSpan = Math.abs(tmTime - item.itime);

                if (tmTime >= item.itime)
                {// 找到比mrO时间早的loc
                    if (curTimeSpan > tmTimeSpan)
                    {
                        curTimeSpan = tmTimeSpan;
                        curItem = item;
                        continue;
                    }
                }

                if (item.itime > tmTime && curItem != null)
                {
                    if ((item.itime - tmTime) < (tmTime - curItem.itime))
                    {
                        return item;
                    }
                    else
                    {
                        return curItem;
                    }
                }
                return item;
            }

            return curItem;
        }

    }

    private class ImsiS1apIDMngExtend
    {
        private List<XdrLable> xdrLableList;
        private int timeExpend = 5 * 60;
        private int TimeSpan = 600;

        public ImsiS1apIDMngExtend()
        {
            xdrLableList = new ArrayList<XdrLable>();
        }

        public void addXdrLocItem(XdrLable lableItem)
        {
            if (lableItem.longitudeGL > 0 && lableItem.testTypeGL <= StaticConfig.TestType_HiRail)
            {
                xdrLableList.add(lableItem);
            }
        }

        public void init()
        {
            Collections.sort(xdrLableList, new Comparator<XdrLable>()
            {
                public int compare(XdrLable a, XdrLable b)
                {
                    return a.itime - b.itime;
                }
            });
        }

        public XdrLable getXdrLoc(int tmTime)
        {
            int curDTTime = timeExpend;
            int curCQTTime = timeExpend;
            int curDTEXTime = timeExpend;
            XdrLable curDTItem = null;
            XdrLable curCQTItem = null;
            XdrLable curDTEXItem = null;
            int tmTimeSpan = tmTime / TimeSpan * TimeSpan;
            int spTime;

            for (XdrLable item : xdrLableList)
            {
                if (item.itime / TimeSpan * TimeSpan != tmTimeSpan)
                {
                    continue;
                }

                if (item.testTypeGL == StaticConfig.TestType_DT && item.longitudeGL > 0)
                {
                    spTime = Math.abs(tmTime - item.loctimeGL);
                    if (spTime < curDTTime)
                    {
                        curDTTime = spTime;
                        curDTItem = item;
                    }
                }

                if (item.testTypeGL == StaticConfig.TestType_CQT && item.longitudeGL > 0)
                {
                    spTime = Math.abs(tmTime - item.loctimeGL);
                    if (spTime < curCQTTime)
                    {
                        curCQTTime = spTime;
                        curCQTItem = item;
                    }
                }

                if (item.testTypeGL == StaticConfig.TestType_DT_EX && item.longitudeGL > 0)
                {
                    spTime = Math.abs(tmTime - item.loctimeGL);
                    if (spTime < curDTEXTime)
                    {
                        curDTEXTime = spTime;
                        curDTEXItem = item;
                    }
                }
            }

            if (curDTItem != null)
            {
                return curDTItem;
            }
            else if (curCQTItem != null)
            {
                return curCQTItem;
            }
            else if (curDTEXItem != null)
            {
                return curDTEXItem;
            }
            return null;
        }

        public XdrLable getXdrLocEx(int tmTime)
        {
            XdrLable[] lables = new XdrLable[2];
            int[] spTimes = new int[2];
            int spTime;

            for (XdrLable item : xdrLableList)
            {
                spTime = tmTime - item.loctimeGL;
                if (spTime > TimeSpan)
                {// 时间比当前xdr时间早10分钟以上
                    continue;
                }
                else if (-spTime > TimeSpan)
                {// 时间比当前xdr时间晚10分钟以上
                    break;
                }

                if (spTime > 0)
                {// 时间比当前xdr时间早，肯定比上一个点更接近当前时间
                    lables[0] = item;
                    spTimes[0] = Math.abs(spTime);
                }
                if (spTime < 0)
                {// 时间比当前xdr时间晚，退出搜索
                    lables[1] = item;
                    spTimes[1] = Math.abs(spTime);
                    break;
                }
            }

            if (lables[0] != null && lables[1] == null)
            {// 只有一个XDR,CQT必须1分钟之内有效
                if (lables[0].testTypeGL != StaticConfig.TestType_CQT || spTimes[0] < 120)
                    return lables[0];
                return null;
            }
            else if (lables[0] == null && lables[1] != null)
            {// 只有一个XDR,CQT必须1分钟之内有效
                if (lables[1].testTypeGL != StaticConfig.TestType_CQT || spTimes[1] < 120)
                    return lables[1];
                return null;
            }
            else if (lables[0] != null && lables[1] != null)
            {
                // testtype是100内的都是正常的测试类型
                if (lables[0].testTypeGL == lables[1].testTypeGL && lables[0].testTypeGL < 100)
                {// 前后类型相同，找时间最近的
                    return spTimes[0] <= spTimes[1] ? lables[0] : lables[1];
                }

                // 判断高铁用户
                if (lables[0].testTypeGL == StaticConfig.TestType_HiRail)
                {
                    return lables[0];
                }

                if (lables[1].testTypeGL == StaticConfig.TestType_HiRail)
                {
                    return lables[1];
                }

                // 判断DT
                if (lables[0].testTypeGL == StaticConfig.TestType_DT)
                {
                    return lables[0];
                }

                if (lables[1].testTypeGL == StaticConfig.TestType_DT)
                {
                    return lables[1];
                }

                // 判断DTEX
                if (lables[0].testTypeGL == StaticConfig.TestType_DT_EX)
                {
                    return lables[0];
                }

                if (lables[1].testTypeGL == StaticConfig.TestType_DT_EX)
                {
                    return lables[1];
                }

                // 判断CQT
                if (lables[0].testTypeGL == StaticConfig.TestType_CQT && spTimes[0] < 120)
                {// 只有一个XDR,CQT必须1分钟之内有效
                    return lables[0];
                }

                if (lables[1].testTypeGL == StaticConfig.TestType_CQT && spTimes[1] < 120)
                {// 只有一个XDR,CQT必须1分钟之内有效
                    return lables[1];
                }
            }
            return null;
        }

    }
}
