package logic.mapred.mroLoc;

import ImeiCapbility.ImeiConfig;
import StructData.*;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import specialUser.SpecialUserConfig;
import util.Func;
import util.MrLocation;
import xdr.lablefill.ResultHelper;
import xdr.locallex.LocItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public class MroLableFileReducer  extends DataDealReducer<CellTimeKey, Text, NullWritable, Text> {
    private MultiOutputMng<NullWritable, Text> mosMng;
    private Text curText = new Text();
    private String path_sample;
    private String path_event;
    private String path_cell;
    private String path_cell_freq;
    private String path_cellgrid;
    private String path_grid;
    private String path_ImsiSampleIndex;
    private String path_ImsiEventIndex;
    private String path_myLog;
    private String path_locMore;
    private String path_mroMore;
    private String path_grid_dt;
    private String path_grid_dt_freq;
    private String path_grid_cqt;
    private String path_grid_cqt_freq;
    private String path_sample_dt;
    private String path_sample_dtex;
    private String path_sample_cqt;
    private String path_sample_index_dt;
    private String path_sample_index_cqt;
    private String path_useract_cell;
    private String path_ten_grid;
    private String path_ten_grid_dt;
    private String path_ten_grid_dt_freq;
    private String path_ten_grid_cqt;
    private String path_ten_grid_cqt_freq;
    private String path_ten_cellgrid;
    public String path_freq_lt_cell_byImei;
    public String path_ten_freq_lt_dtGrid_byImei;
    public String path_ten_freq_lt_cqtGrid_byImei;
    public String path_freq_dx_cell_byImei;
    public String path_ten_freq_dx_dtGrid_byImei;
    public String path_ten_freq_dx_cqtGrid_byImei;
    public static String path_cellgrid_dt_10;
    public static String path_cellGrid_cqt_10;
    public static String path_locLib;
    public static String path_xdr_locLib;
    public static String path_special_user_sample;
    public static String path_indoor_err_table;
    public String path_mr_err_evt;

    private String outpath_table;
    private String dateStr;
    private UserMrStat userStat;
    private TypeResult typeResult;

    // mdt
    private UserMdtStat userMdtStat;
    private TypeResult mdtTypeResult;
    private Context context;
    private long tempEci;// 记录上一个eci
    private CellBuildInfo cellBuild;
    private CellBuildWifi cellBuildWifi;
    protected static final Log LOG = LogFactory.getLog(MroLableFileReducers.MroDataFileReducers.class);
    private final int TimeSpan = 600;// 10分钟间隔
    private StringBuilder tmSb = new StringBuilder();

    private StatDeal statDeal;
    private StatDeal_DT statDeal_DT;
    private StatDeal_CQT statDeal_CQT;

    private XdrLableMng xdrLableMng;
    private UserActStatMng userActStatMng;
    private static TypeInfoMng typeInfoMng;
    private static TypeInfoMng mdtTypeInfoMng;
    private UserProp userProp;
    private UserLocer userLocer;
    private FigureFixedOutput figureMroFix;// 指纹库定位结果输出
    private long currEci = 0;

    private int IndoorGridSize = 0;
    private int OutdoorGridSize = 0;
    LteCellInfo cellInfo;
    private long count;

    private DataAdapterConf.ParseItem parseItem;
    private DataAdapterReader dataAdapterReader;
    HashMap<String, ArrayList<MroOrigDataMT>> map = new HashMap<String, ArrayList<StructData.MroOrigDataMT>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        MainModel.GetInstance().setConf(conf);
//        figureMroFix = new FigureFixedOutput(context, conf);
//        figureMroFix.setup();


        outpath_table = conf.get("mapreduce.job.oupath");
        String tempData = conf.get("mapreduce.job.date");
        if (tempData != null)
        {
            dateStr = tempData.replace("01_", "");
        }
        path_sample = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample");
        path_event = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_event");
        path_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell");
        path_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cell_freq");
        path_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid");
        path_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid");
        path_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiSampleIndex");
        path_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ImsiEventIndex");
        path_myLog = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_myLog");
        path_locMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locMore");
        path_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_mroMore");

        path_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt");
        path_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_dt_freq");
        path_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt");
        path_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_grid_cqt_freq");
        path_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dt");
        path_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_dtex");
        path_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_cqt");
        path_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_dt");
        path_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_sample_index_cqt");

        path_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_useract_cell");

        path_ten_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid");
        path_ten_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt");
        path_ten_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_dt_freq");
        path_ten_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt");
        path_ten_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_grid_cqt_freq");
        path_ten_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_cellgrid");

        path_freq_lt_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_freq_lt_cell_byImei");
        path_ten_freq_lt_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_dtGrid_byImei");
        path_ten_freq_lt_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_lt_cqtGrid_byImei");

        path_freq_dx_cell_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_freq_dx_cell_byImei");
        path_ten_freq_dx_dtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_dtGrid_byImei");
        path_ten_freq_dx_cqtGrid_byImei = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_ten_freq_dx_cqtGrid_byImei");

        path_cellgrid_dt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellgrid_dt_10");
        path_cellGrid_cqt_10 = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_cellGrid_cqt_10");

        path_locLib = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_locLib");
        path_xdr_locLib = conf.get("mastercom.mroxdrmerge.mro.locfillex.path_xdr_loclib");
        path_special_user_sample = conf.get("mastercom.mroxdrmerge.mro.special.sample");
        path_indoor_err_table = conf.get("mastercom.mroxdrmerge.path_indoor_err_table");
//20171101 add mr 故障事件
        path_mr_err_evt = conf.get("mastercom.mroxdrmerge.path_mr_err_evt");

        this.context = context;

        // 初始化输出控制
        if (path_sample.contains(":"))
            mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
        else
            mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

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

        // fgottstat output
        typeInfoMng = new TypeInfoMng();
        MroNewTableStat.getOutPutPackage(mosMng, typeInfoMng, outpath_table, dateStr);

        // mdt output
        mdtTypeInfoMng = new TypeInfoMng();
        MdtNewTableStat.getOutPutPackage(mosMng, mdtTypeInfoMng, outpath_table, dateStr);

        mosMng.init();
        ////////////////////

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

        ////////////////////

        statDeal = new StatDeal(mosMng);
        statDeal_DT = new StatDeal_DT(mosMng);
        statDeal_CQT = new StatDeal_CQT(mosMng);
        xdrLableMng = new XdrLableMng();
        userActStatMng = new UserActStatMng();

        // fgottstat
        typeResult = new TypeResult(typeInfoMng);
        userStat = new UserMrStat(typeResult);

        // mdt new table
        mdtTypeResult = new TypeResult(mdtTypeInfoMng);
        userMdtStat = new UserMdtStat(mdtTypeResult);

        parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC");
        if (parseItem == null)
        {
            throw new IOException("parse item do not get.");
        }
        dataAdapterReader = new DataAdapterReader(parseItem);

        // 打印状态日志
        LOGHelper.GetLogger().writeLog(LogType.info, "cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        try
        {
            LOGHelper.GetLogger().writeLog(LogType.info, "begin	 cleanup:");
//            outUserData();
//            outAllData();
            figureMroFix.cleanup();
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
    {}


}
