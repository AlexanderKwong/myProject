package logic.deal.mroLoc.join;

import base.IModel;
import base.deal.impl.JoinDeal;
import cellconfig.CellBuildInfo;
import cellconfig.CellBuildWifi;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.util.IWriteLogCallBack;
import jan.util.LOGHelper;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.WeakHashMap;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class CellMng implements JoinDeal.ModelMng {

    private long eci;
    public LteCellInfo cellInfo;
    public CellBuildInfo cellBuild;
    public CellBuildWifi cellBuildWifi;

    @Override
    public List get(JoinDeal.JoinCondition joinCondition) {
        return null;
    }

    private CellMng(){}

    public static class Builder implements JoinDeal.ModelMngBuilder {
        //for cache
        private static WeakHashMap<Long, CellMng> cellMngCache = new WeakHashMap<>();

        CellMng cellMng;

        public Builder(long eci){
            cellMng = new CellMng();
            cellMng.eci = eci;
        }

        @Override
        public JoinDeal.ModelMngBuilder append(IModel model) {
            return this;
        }

        @Override
        public JoinDeal.ModelMng create() {
            try{
                cellMng.cellInfo = CellConfig.GetInstance().getLteCell(cellMng.eci);
                if (cellMng.cellInfo == null)// cell统计需要全量，不能抛弃
                {
                    cellMng.cellInfo = new LteCellInfo();
                    LOGHelper.GetLogger().writeLog(
                            IWriteLogCallBack.LogType.info,
                            "gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:"
                                    + cellMng.eci + "  enbid:" + cellMng.eci / 256 + " cellid:" + cellMng.eci % 256);
                }
                // 初始化小区楼宇表
                cellMng.cellBuild = new CellBuildInfo();
                if (!cellMng.cellBuild.loadCellBuild(new Configuration(), cellMng.eci, cellMng.cellInfo.cityid))
                {
                    LOGHelper.GetLogger().writeLog(
                            IWriteLogCallBack.LogType.error,
                            "cellbuild  init error 请检查！eci:" + cellMng.eci + " map.size:"
                                    + cellMng.cellBuild.getCellBuildMap().size());
                }
                // 初始化小区楼宇wifi
                cellMng.cellBuildWifi = new CellBuildWifi();
                if (!cellMng.cellBuildWifi.loadBuildWifi(new Configuration(), cellMng.eci, cellMng.cellInfo.cityid))
                {
                    LOGHelper.GetLogger().writeLog(
                            IWriteLogCallBack.LogType.error,
                            "cellbuildwifi  init error 请检查！eci:" + cellMng.eci + " map.size:"
                                    + cellMng.cellBuildWifi.getCellBuildWifiMap().size());
                }
            }catch (Exception e){
                e.printStackTrace();
            }

            return cellMng;
        }
    }
}
