package locuser_v2;

import java.util.ArrayList;
import java.util.List;

import StructData.SIGNAL_MR_All;

public class MrSam
{
    public int cityid = 0;
    public int itime = 0; // 聚合时间点
    public long s1apid = 0;
    public String mrotype = "";
    public int ta = -1;
    public int aoa = -1000000;
    public int sinr = -1000000;
    public int isroad = 0;
    public SIGNAL_MR_All mall = null;
    public Mrcell scell = new Mrcell();
    public List<Mrcell> ncells = new ArrayList<Mrcell>();
}
