package logic.deal.mroLoc;

import base.IModel;
import base.deal.impl.JoinDeal;
import logic.deal.mroLoc.join.CellMng;
import mro.lablefill.XdrLable;
import mro.lablefillex.WifiFixed;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class XdrCellJoinDeal extends JoinDeal<IModel> {
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
    public IModel deal(IModel o) throws Exception {
        if(o instanceof XdrLable){
            XdrLable xdrLable = (XdrLable)o;
            xdrLable.ibuildid = cellMng.cellBuild.getBuildId(xdrLable.longitudeGL, xdrLable.latitudeGL);
            xdrLable.iheight = WifiFixed.returnLevel(cellMng.cellBuildWifi, xdrLable.wifiName, xdrLable.ibuildid);
        }
        return o;
    }

    @Override
    public void flush() {

    }
}
