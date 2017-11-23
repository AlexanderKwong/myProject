package logic.deal;

import base.IModel;
import base.deal.exception.ContinueException;
import cellconfig.CellBuildInfo;
import cellconfig.CellBuildWifi;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.util.IWriteLogCallBack;
import jan.util.LOGHelper;
import model.DT_Sample_4G;
import base.deal.JoinDeal;
import model.SIGNAL_MR_All;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by Kwong on 2017/11/21.
 */
public class MrCellJoinDeal extends JoinDeal<IModel>{

    private long eci;
    private LteCellInfo cellInfo;
    private CellBuildInfo cellBuild;
    private CellBuildWifi cellBuildWifi;

    public void init(long eci){
        if (this.eci != eci){
            this.eci = eci;
            try{
                cellCfgInitialize();
            }catch (Exception e){
                throw new IllegalArgumentException();
            }
        }
    }
    
    private void cellCfgInitialize() throws Exception{
        if (cellInfo == null)// cell统计需要全量，不能抛弃
        {
            cellInfo = new LteCellInfo();
            LOGHelper.GetLogger().writeLog(
                    IWriteLogCallBack.LogType.info,
                    "gongcansize:" + CellConfig.GetInstance().getlteCellInfoMapSize() + "  gongcan no eci:"
                            + this.eci + "  enbid:" + this.eci / 256 + " cellid:" + this.eci % 256);
        }
        // 初始化小区楼宇表
        cellBuild = new CellBuildInfo();
        if (!cellBuild.loadCellBuild(new Configuration(), this.eci, cellInfo.cityid))
        {
            LOGHelper.GetLogger().writeLog(
                    IWriteLogCallBack.LogType.error,
                    "cellbuild  init error 请检查！eci:" + this.eci + " map.size:"
                            + cellBuild.getCellBuildMap().size());
        }
        // 初始化小区楼宇wifi
        cellBuildWifi = new CellBuildWifi();
        if (!cellBuildWifi.loadBuildWifi(new Configuration(), this.eci, cellInfo.cityid))
        {
            LOGHelper.GetLogger().writeLog(
                    IWriteLogCallBack.LogType.error,
                    "cellbuildwifi  init error 请检查！eci:" + this.eci + " map.size:"
                            + cellBuildWifi.getCellBuildWifiMap().size());
        }
    }

    @Override
    public IModel deal(IModel o) throws Exception {
        if(o instanceof SIGNAL_MR_All){
            SIGNAL_MR_All mroItem = (SIGNAL_MR_All)o;
            if (mroItem == null || mroItem.tsc == null || mroItem.tsc.MmeUeS1apId <= 0 || mroItem.tsc.Eci <= 0 || mroItem.tsc.beginTime <= 0)
                throw new ContinueException();
            // 附上地市id
            mroItem.tsc.cityID = cellInfo.cityid;

        }
        return o;
    }

    @Override
    public void flush() {

    }
}
