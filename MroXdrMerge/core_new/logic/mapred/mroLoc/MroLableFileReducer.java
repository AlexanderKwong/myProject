package logic.mapred.mroLoc;

import ImeiCapbility.ImeiConfig;
import StructData.*;
import base.IDeal;
import base.IGroupKey;
import base.IModel;
import base.deal.impl.DealBuilder;
import base.deal.impl.PrepareDeal;
import cellconfig.CellBuildInfo;
import cellconfig.CellBuildWifi;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf;
import jan.util.DataAdapterReader;
import jan.util.GisFunction;
import jan.util.LOGHelper;
import locuser.UserProp;
import locuser_v2.UserLocer;
import logic.deal.mroLoc.*;
import logic.deal.mroLoc.join.FgFilterDeal;
import mdtstat.MdtNewTableStat;
import mdtstat.UserMdtStat;
import mro.evt.EventData;
import mro.evt.MrErrorEventData;
import mro.lablefill.*;
import mro.lablefillex.MroLableFileReducers;
import mro.lablefillex.MroLocStat;
import mro.lablefillex.WifiFixed;
import mro.lablefillex_uemro.FigureFixedOutput;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import mrstat.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import specialUser.SpecialUserConfig;
import util.Func;
import util.MrLocation;
import xdr.lablefill.ResultHelper;
import xdr.locallex.LocItem;

import java.io.IOException;
import java.util.*;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class MroLableFileReducer  extends DataDealReducer<CellTimeKey, Text, NullWritable, Text> {
    MultiOutputMng<NullWritable, Text> mosMng = null;

    private Context context;
    protected static final Log LOG = LogFactory.getLog(MroLableFileReducers.MroDataFileReducers.class);

    private IDeal<Tuple2<CellTimeKey, String>, DT_Sample_4G> deal;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        MainModel.GetInstance().setConf(conf);
        this.context = context;
        loadConfig();

        TypeInfoMng typeInfoMng = null;
        TypeInfoMng mdtTypeInfoMng = null;
        initOutputMng(conf, mosMng, typeInfoMng, mdtTypeInfoMng);

        deal = new DealBuilder<>(new PrepareDeal<CellTimeKey>()) //组装实体
                .append(new XdrLocOutDeal(mosMng))//XDR位置库吐出
                .append(new XdrCellJoinDeal())//XDR关联小区
                .append(new MrXdrJoinDeal())//MR关联XDR
                .append(new MrCellJoinDeal())//MR关联小区
                .append(new MrLocOutDeal(mosMng))//吐出MR位置库
                .append(new MrToSampleMapDeal())//MR 转 Sample
                .append(new BeijingSpecUserOutDeal(mosMng))//吐出 北京特殊用户
                .append(new FgOttStatDeal(mosMng, typeInfoMng, mdtTypeInfoMng))//统计新表
                .append(new FgFilterDeal())//过滤掉 指纹库定位的数据
                .append(new SampleOutDeal(mosMng))//吐出采样点
                .append(new KpiStatDeal(mosMng))//统计 KPI
                .create();

    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        try
        {
            LOGHelper.GetLogger().writeLog(LogType.info, "begin cleanup:");
//            outUserData();
//            outAllData();
//            figureMroFix.cleanup();
            LOGHelper.GetLogger().writeLog(LogType.info, "end cleanup:");
        }
        catch (Exception e)
        {
            LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
        }

        super.cleanup(context);

        mosMng.close();
    }

    @Override
    public void reduce(CellTimeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Tuple2<CellTimeKey, String> kv = new Tuple2<>(null, null);
        for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();){
            String valStr = iterator.next().toString();
            try {
                deal.deal(new Tuple2<CellTimeKey, String>(key, valStr));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        deal.flush();
    }

    /**
     * 初始化输出
     * @param conf
     * @param mosMng
     * @param typeInfoMng
     * @param mdtTypeInfoMng
     * @throws IOException
     * @throws InterruptedException
     */
    private void initOutputMng(Configuration conf, MultiOutputMng<NullWritable, Text> mosMng, TypeInfoMng typeInfoMng, TypeInfoMng mdtTypeInfoMng) throws IOException, InterruptedException{
        String path_sample = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample");
        String path_event = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_event");
        String path_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell");
        String path_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell_freq");
        String path_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid");
        String path_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid");
        String path_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiSampleIndex");
        String path_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiEventIndex");
        String path_myLog = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_myLog");
        String path_locMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locMore");
        String path_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mroMore");

        String path_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt");
        String path_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt_freq");
        String path_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt");
        String path_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt_freq");
        String path_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dt");
        String path_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dtex");
        String path_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_cqt");
        String path_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_dt");
        String path_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_cqt");

        String path_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_useract_cell");

        String path_ten_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid");
        String path_ten_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt");
        String path_ten_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt_freq");
        String path_ten_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt");
        String path_ten_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt_freq");
        String path_ten_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_cellgrid");

        String path_freq_lt_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_freq_lt_cell_byImei");
        String path_ten_freq_lt_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_dtGrid_byImei");
        String path_ten_freq_lt_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_cqtGrid_byImei");

        String path_freq_dx_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_freq_dx_cell_byImei");
        String path_ten_freq_dx_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_dtGrid_byImei");
        String path_ten_freq_dx_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_cqtGrid_byImei");

        String path_cellgrid_dt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid_dt_10");
        String path_cellGrid_cqt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellGrid_cqt_10");

        String path_locLib = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locLib");
        String path_xdr_locLib = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_xdr_loclib");
        String path_special_user_sample = conf.get("mastercom.mroxdrmerge.mro.special.sample");
        String path_indoor_err_table = conf.get("mastercom.mroxdrmerge.path_indoor_err_table");
//20171101 add mr 故障事件
        String path_mr_err_evt = conf.get("mastercom.mroxdrmerge.path_mr_err_evt");

        // 初始化输出控制
        if (path_sample.contains(":"))
            mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
        else
            mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

        String dateStr = null,outpath_table = null;
        outpath_table = conf.get("mapreduce.job.oupath");
        String tempData = conf.get("mapreduce.job.date");
        if (tempData != null)
        {
            dateStr = tempData.replace("01_", "");
        }

        // fgottstat output
        typeInfoMng = new TypeInfoMng();
        MroNewTableStat.getOutPutPackage(mosMng, typeInfoMng, outpath_table, dateStr);

        // mdt output
        mdtTypeInfoMng = new TypeInfoMng();
        MdtNewTableStat.getOutPutPackage(mosMng, mdtTypeInfoMng, outpath_table, dateStr);

        mosMng.SetOutputPath("mrosample", path_sample);
        mosMng.SetOutputPath("mroevent", path_event);
        mosMng.SetOutputPath("mrocell", path_cell);
        mosMng.SetOutputPath("mrocellfreq", path_cell_freq);
        mosMng.SetOutputPath("mrocellgrid", path_cellgrid);
        mosMng.SetOutputPath("mrogrid", path_grid);
        mosMng.SetOutputPath("imsisampleindex", path_ImsiSampleIndex);
        mosMng.SetOutputPath("imsieventindex", path_ImsiEventIndex);
        mosMng.SetOutputPath("myLog", path_myLog);
        mosMng.SetOutputPath("locMore", path_locMore);
        mosMng.SetOutputPath("mroMore", path_mroMore);

        mosMng.SetOutputPath("griddt", path_grid_dt);
        mosMng.SetOutputPath("griddtfreq", path_grid_dt_freq);
        mosMng.SetOutputPath("gridcqt", path_grid_cqt);
        mosMng.SetOutputPath("gridcqtfreq", path_grid_cqt_freq);
        mosMng.SetOutputPath("sampledt", path_sample_dt);
        mosMng.SetOutputPath("sampledtex", path_sample_dtex);
        mosMng.SetOutputPath("samplecqt", path_sample_cqt);
        mosMng.SetOutputPath("sampleindexdt", path_sample_index_dt);
        mosMng.SetOutputPath("sampleindexcqt", path_sample_index_cqt);

        mosMng.SetOutputPath("useractcell", path_useract_cell);

        mosMng.SetOutputPath("tenmrogrid", path_ten_grid);
        mosMng.SetOutputPath("tengriddt", path_ten_grid_dt);
        mosMng.SetOutputPath("tengriddtfreq", path_ten_grid_dt_freq);
        mosMng.SetOutputPath("tengridcqt", path_ten_grid_cqt);
        mosMng.SetOutputPath("tengridcqtfreq", path_ten_grid_cqt_freq);
        mosMng.SetOutputPath("tenmrocellgrid", path_ten_cellgrid);

        mosMng.SetOutputPath("LTfreqcellByImei", path_freq_lt_cell_byImei);
        mosMng.SetOutputPath("LTtenFreqByImeiDt", path_ten_freq_lt_dtGrid_byImei);
        mosMng.SetOutputPath("LTtenFreqByImeiCqt", path_ten_freq_lt_cqtGrid_byImei);

        mosMng.SetOutputPath("DXfreqcellByImei", path_freq_dx_cell_byImei);
        mosMng.SetOutputPath("DXtenFreqByImeiDt", path_ten_freq_dx_dtGrid_byImei);
        mosMng.SetOutputPath("DXtenFreqByImeiCqt", path_ten_freq_dx_cqtGrid_byImei);

        mosMng.SetOutputPath("tencellgriddt", path_cellgrid_dt_10);
        mosMng.SetOutputPath("tencellgridcqt", path_cellGrid_cqt_10);
        mosMng.SetOutputPath("loclib", path_locLib);
        mosMng.SetOutputPath("xdrloclib", path_xdr_locLib);

        mosMng.SetOutputPath("mrvap", path_special_user_sample);
        mosMng.SetOutputPath("indoorErr", path_indoor_err_table);

        mosMng.SetOutputPath("mroErrEvt", path_mr_err_evt);

        mosMng.init();
    }

    /**
     * 加载小区等配置
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadConfig() throws IOException, InterruptedException{
        // 初始化小区的信息
        if (!CellConfig.GetInstance().loadLteCell(conf))
        {
            LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
            throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
        }
        // 初始化imei表
        if (!ImeiConfig.GetInstance().loadImeiCapbility(conf))
        {
            LOGHelper.GetLogger().writeLog(LogType.info, "imeiconfig  init error 请检查！");
        }

        // 加载特例用户
        if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
        {
            if (!SpecialUserConfig.GetInstance().loadSpecialuser(conf, true))
            {
                LOGHelper.GetLogger().writeLog(LogType.error, "specialUser init error 请检查！");
            }
        }
    }
}
