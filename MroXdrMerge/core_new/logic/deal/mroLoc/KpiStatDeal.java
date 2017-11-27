package logic.deal.mroLoc;

import StructData.StaticConfig;
import base.Cacheable;
import base.IDataOutputer;
import base.deal.impl.StatDeal;
import base.deal.impl.exception.ContinueException;
import StructData.DT_Sample_4G;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.IWriteLogCallBack;
import jan.util.LOGHelper;
import mro.lablefill.StatDeal_CQT;
import mro.lablefill.StatDeal_DT;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStatMng;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class KpiStatDeal  extends StatDeal<DT_Sample_4G> implements Cacheable{

    private mro.lablefill.StatDeal statDeal;
    private UserActStatMng userActStatMng;
    private StatDeal_DT statDeal_DT;
    private StatDeal_CQT statDeal_CQT;

    private MultiOutputMng<NullWritable, Text> mosMng;

    private Text curText;

    public KpiStatDeal(IDataOutputer dataOutputer){
        mosMng = (MultiOutputMng<NullWritable, Text>)dataOutputer;
        statDeal = new mro.lablefill.StatDeal(mosMng);
        statDeal_DT = new StatDeal_DT(mosMng);
        statDeal_CQT = new StatDeal_CQT(mosMng);
        userActStatMng = new UserActStatMng();

        curText = new Text();
    }

    @Override
    public void flush() {
// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
      /*  statDeal.outDealingData();
        statDeal_DT.outDealingData();
        statDeal_CQT.outDealingData();

        // 如果用户数据大于10000个，就吐出去先
        if (userActStatMng.getUserActStatMap().size() > 10000)
        {
            userActStatMng.finalStat();

            // 用户行动信息输出
            StringBuffer sb = new StringBuffer();
            for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
            {
                try
                {
                    sb.delete(0, sb.length());

                    String TabMark = "\t";
                    for (UserActStat.UserActTime userActTime : userActStat.userActTimeMap.values())
                    {
                        for (UserActStat.UserCellAll userActAll : userActTime.userCellAllMap.values())
                        {
                            sb.delete(0, sb.length());

                            sb.append(0);// cityid
                            sb.append(TabMark);
                            sb.append(userActStat.imsi);
                            sb.append(TabMark);
                            sb.append(userActStat.msisdn);
                            sb.append(TabMark);
                            sb.append(userActTime.stime);
                            sb.append(TabMark);
                            sb.append(userActTime.etime);
                            sb.append(TabMark);

                            // 主服小区
                            UserActStat.UserCell mainUserCell = userActAll.getMainUserCell();
                            sb.append(userActAll.eci);
                            sb.append(TabMark);
                            sb.append(0);
                            sb.append(TabMark);
                            sb.append(userActAll.eci);
                            sb.append(TabMark);
                            sb.append(mainUserCell.rsrpSum);
                            sb.append(TabMark);
                            sb.append(mainUserCell.rsrpTotal);
                            sb.append(TabMark);
                            sb.append(mainUserCell.rsrpMaxMark);
                            sb.append(TabMark);
                            sb.append(mainUserCell.rsrpMinMark);

                            curText.set(sb.toString());
                            mosMng.write("useractcell", NullWritable.get(), curText);

                            // 邻区
                            List<UserActStat.UserCell> userCellList = userActAll.getUserCellList();
                            int sn = 1;
                            for (UserActStat.UserCell userCell : userCellList)
                            {
                                if (userCell.eci == userActAll.eci)
                                {
                                    continue;
                                }

                                sb.delete(0, sb.length());
                                sb.append(0);// cityid
                                sb.append(TabMark);
                                sb.append(userActStat.imsi);
                                sb.append(TabMark);
                                sb.append(userActStat.msisdn);
                                sb.append(TabMark);
                                sb.append(userActTime.stime);
                                sb.append(TabMark);
                                sb.append(userActTime.etime);
                                sb.append(TabMark);

                                sb.append(userActAll.eci);
                                sb.append(TabMark);
                                sb.append(sn);
                                sb.append(TabMark);
                                sb.append(userCell.eci);
                                sb.append(TabMark);
                                sb.append(userCell.rsrpSum);
                                sb.append(TabMark);
                                sb.append(userCell.rsrpTotal);
                                sb.append(TabMark);
                                sb.append(userCell.rsrpMaxMark);
                                sb.append(TabMark);
                                sb.append(userCell.rsrpMinMark);

                                curText.set(sb.toString());
                                mosMng.write("useractcell", NullWritable.get(), curText);
                                sn++;
                            }

                        }
                    }
                }
                catch (Exception e)
                {
                    LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "user action error", e);
                }
            }

            userActStatMng = new UserActStatMng();
        }*/
      flushAllCache();
    }

    @Override
    public boolean stat(DT_Sample_4G sample) throws Exception{
        // cpe不参与kpi运算
        if (sample.testType == StaticConfig.TestType_CPE)
        {
            throw new ContinueException();
        }
        statDeal.dealSample(sample);
        userActStatMng.stat(sample);

        // StaticConfig.TestType_DT_EX 不参与运算
        if (sample.testType == StaticConfig.TestType_DT)
        {
            statDeal_DT.dealSample(sample);
        }

        if (sample.testType == StaticConfig.TestType_CQT)
        {
            statDeal_CQT.dealSample(sample);
        }
        return true;
    }

    @Override
    public int flushThreshold() {
        return 10000;
    }

    @Override
    public void flushAllCache() {
        statDeal.outAllData();
        statDeal_DT.outAllData();
        statDeal_CQT.outAllData();

        userActStatMng.finalStat();
        // 用户行动信息输出
        for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
        {
            try
            {
                StringBuffer sb = new StringBuffer();
                String TabMark = "\t";
                for (UserActStat.UserActTime userActTime : userActStat.userActTimeMap.values())
                {
                    for (UserActStat.UserCellAll userActAll : userActTime.userCellAllMap.values())
                    {
                        sb.delete(0, sb.length());

                        sb.append(0);// cityid
                        sb.append(TabMark);
                        sb.append(userActStat.imsi);
                        sb.append(TabMark);
                        sb.append(userActStat.msisdn);
                        sb.append(TabMark);
                        sb.append(userActTime.stime);
                        sb.append(TabMark);
                        sb.append(userActTime.etime);
                        sb.append(TabMark);

                        // 主服小区
                        UserActStat.UserCell mainUserCell = userActAll.getMainUserCell();
                        sb.append(userActAll.eci);
                        sb.append(TabMark);
                        sb.append(0);
                        sb.append(TabMark);
                        sb.append(userActAll.eci);
                        sb.append(TabMark);
                        sb.append(mainUserCell.rsrpSum);
                        sb.append(TabMark);
                        sb.append(mainUserCell.rsrpTotal);
                        sb.append(TabMark);
                        sb.append(mainUserCell.rsrpMaxMark);
                        sb.append(TabMark);
                        sb.append(mainUserCell.rsrpMinMark);

                        curText.set(sb.toString());
                        mosMng.write("useractcell", NullWritable.get(), curText);

                        // 邻区
                        List<UserActStat.UserCell> userCellList = userActAll.getUserCellList();
                        int sn = 1;
                        for (UserActStat.UserCell userCell : userCellList)
                        {
                            if (userCell.eci == userActAll.eci)
                            {
                                continue;
                            }

                            sb.delete(0, sb.length());
                            sb.append(0);// cityid
                            sb.append(TabMark);
                            sb.append(userActStat.imsi);
                            sb.append(TabMark);
                            sb.append(userActStat.msisdn);
                            sb.append(TabMark);
                            sb.append(userActTime.stime);
                            sb.append(TabMark);
                            sb.append(userActTime.etime);
                            sb.append(TabMark);

                            sb.append(userActAll.eci);
                            sb.append(TabMark);
                            sb.append(sn);
                            sb.append(TabMark);
                            sb.append(userCell.eci);
                            sb.append(TabMark);
                            sb.append(userCell.rsrpSum);
                            sb.append(TabMark);
                            sb.append(userCell.rsrpTotal);
                            sb.append(TabMark);
                            sb.append(userCell.rsrpMaxMark);
                            sb.append(TabMark);
                            sb.append(userCell.rsrpMinMark);

                            curText.set(sb.toString());
                            mosMng.write("useractcell", NullWritable.get(), curText);
                            sn++;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error, "user action error", e);
            }
        }
    }
}
