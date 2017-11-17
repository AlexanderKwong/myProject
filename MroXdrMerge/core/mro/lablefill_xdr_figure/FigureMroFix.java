package mro.lablefill_xdr_figure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import StructData.DT_Sample_4G;
import StructData.NC_LTE;
import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import cellconfig.CellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.GisFunction;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import mro.lablefill.StatDeal;
import mro.lablefill.StatDeal_CQT;
import mro.lablefill.StatDeal_DT;
import mro.lablefill.UserActStat;
import mro.lablefill.UserActStatMng;
import mro.lablefill_xdr_figure.MroLableFileReducers.MroDataFileReducers;
import mro.lablefill.UserActStat.UserActTime;
import mro.lablefill.UserActStat.UserCell;
import mro.lablefill.UserActStat.UserCellAll;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import util.MrLocation;
import xdr.lablefill.ResultHelper;

public class FigureMroFix
{
	public HashMap<Integer, HashMap<GridKey, FigureRecord>> figureData = new HashMap<Integer, HashMap<GridKey, FigureRecord>>();
	public HashMap<GridKey, ArrayList<FigureRecord>> Forty_tenGridMap;
	public HashMap<Integer, HashMap<GridKey, FigureRecord>> eci_rsrp_gridMap;

	public double pianyiNum;
	public int Range;
	public int CellNum;
	public double percent;
	public int figureFixFlag;
	public long figure_eci = 0;// 记录这一包指纹的eci
	public HashMap<Long, HashMap<Integer, ArrayList<SIGNAL_MR_All>>> mmeues1apidDtCqtMap = new HashMap<Long, HashMap<Integer, ArrayList<SIGNAL_MR_All>>>();// 指纹定位用户按用户定位
	public List<SIGNAL_MR_All> FigureMroItemList = new ArrayList<SIGNAL_MR_All>();

	private MultiOutputMng<NullWritable, Text> mosMng;
	private Text curText = new Text();
	public Context context;
	public Configuration conf;

	/////////////////////// 指纹库定位输出路径///////////////////
	private String fpath_sample;
	private String fpath_event;
	private String fpath_cell;
	private String fpath_cell_freq;
	private String fpath_cellgrid;
	private String fpath_grid;
	private String fpath_ImsiSampleIndex;
	private String fpath_ImsiEventIndex;
	private String fpath_myLog;
	private String fpath_locMore;
	private String fpath_mroMore;

	private String fpath_grid_dt;
	private String fpath_grid_dt_freq;
	private String fpath_grid_cqt;
	private String fpath_grid_cqt_freq;
	private String fpath_sample_dt;
	private String fpath_sample_dtex;
	private String fpath_sample_cqt;
	private String fpath_sample_index_dt;
	private String fpath_sample_index_cqt;

	private String fpath_ten_grid;
	private String fpath_ten_grid_dt;
	private String fpath_ten_grid_dt_freq;
	private String fpath_ten_grid_cqt;
	private String fpath_ten_grid_cqt_freq;
	private String fpath_ten_cellgrid;

	private String fpath_useract_cell;

	protected final Log LOG = LogFactory.getLog(MroDataFileReducers.class);

	private StringBuilder tmSb = new StringBuilder();

	private StatDeal statDeal;
	private StatDeal_DT statDeal_DT;
	private StatDeal_CQT statDeal_CQT;
	private UserActStatMng userActStatMng;

	public FigureMroFix(Context context, Configuration conf)
	{
		this.context = context;
		this.conf = conf;
	}

	/**
	 * 指纹库定位用到的一些参数
	 */
	public void init()
	{
		pianyiNum = Double.parseDouble(MainModel.GetInstance().getAppConfig().getPianyiNum());
		Range = Integer.parseInt(MainModel.GetInstance().getAppConfig().getRange());
		CellNum = Integer.parseInt(MainModel.GetInstance().getAppConfig().getCellNum());
		percent = Double.parseDouble(MainModel.GetInstance().getAppConfig().getPercent());
		figureFixFlag = Integer.parseInt(MainModel.GetInstance().getAppConfig().getFigureFixdFlag());
	}

	public void prapFigure(LteCellInfo cellInfo, long eci)
	{
		if (cellInfo.indoor != 1 && figureFixFlag == 1)// 为指纹库定位组织好数据
		{
			returnEci_rsrp_gridMap(eci);// 将40的栅格按照rsrp分组
			returnForty_tenGridMap();// 将10的栅格和40的格子映射
		}
	}

	public void figureFixed(LteCellInfo cellInfo, SIGNAL_MR_All mroItem)
	{
		if (figureFixFlag == 1)
		{
			if (cellInfo.indoor == 1)// 室分定位
			{
				getIndoorSample(mroItem, cellInfo);
				FigureMroItemList.add(mroItem);// 室分定位结果
			}
			else// 指纹库定位
			{
				boolean figureFixedFlag = getSample(mroItem);
				if (figureFixedFlag)
				{
					getMmeDtOrCqtMap(mroItem);// 指纹库定位结果
				}
			}
		}
	}

	/**
	 * 组织指纹库
	 * 
	 * @param key
	 * @param values
	 */
	public void loadFigureData(long figureEci, Iterable<Text> values)
	{
		figureData.clear();// 清空指纹
		figure_eci = figureEci;// 记录这个reduce的指纹eci
		// 装进新的指纹数据
		for (Text s : values)
		{
			try
			{
				fullFigureMap(new FigureRecord(s.toString()));
			}
			catch (Exception e)
			{
				continue;
			}
		}
	}

	/**
	 * 将reduce中一批指纹按照10/40+40low(去重) 组装起来
	 * 
	 * @param figureData
	 * @param gridFigure
	 */
	public void fullFigureMap(FigureRecord gridFigure)
	{
		String temp = gridFigure.getType();
		GridKey gridkey = gridFigure.returnGridKey();
		int key = 0;
		if (temp.equals("10"))
		{
			key = 10;
		}
		else if (temp.equals("40"))
		{
			key = 40;
		}
		else if (temp.equals("40low"))
		{
			key = 401;// 40low
		}
		HashMap<GridKey, FigureRecord> gridMap;
		if (key == 401)
		{
			gridMap = figureData.get(40);
		}
		else
		{
			gridMap = figureData.get(key);
		}
		if (gridMap == null)
		{
			gridMap = new HashMap<GridKey, FigureRecord>();
			if (key == 10)
			{
				figureData.put(10, gridMap);
			}
			else// 40、40low都放到40里
			{
				figureData.put(40, gridMap);
			}
		}
		FigureRecord tempGrid = gridMap.get(gridkey);
		if (tempGrid == null)
		{
			gridMap.put(gridkey, gridFigure);
		}
		else if (key == 40)// 40高精度可以覆盖40低精度
		{
			gridMap.put(gridkey, gridFigure);
		}
	}

	/**
	 * 在40*40栅格基础上得到符合条件的10*10栅格
	 * 
	 * @param fixedFortyGridResult
	 * @param mr_rsrp
	 * @param eci
	 * @return
	 */
	public HashMap<GridKey, FigureRecord> returnTenGridMap(OneGridResult fixedFortyGridResult, int mr_rsrp, long eci)
	{
		HashMap<GridKey, FigureRecord> suitTenGrididMap = new HashMap<GridKey, FigureRecord>();
		long longitude = fixedFortyGridResult.getIlongitude();
		long latitude = fixedFortyGridResult.getIlatitude();
		int level = fixedFortyGridResult.getLevel();
		GridKey mergeGridkey = new GridKey(longitude, latitude, level);
		ArrayList<FigureRecord> tenlist = Forty_tenGridMap.get(mergeGridkey);
		if (tenlist == null)
		{
			return suitTenGrididMap;
		}
		for (FigureRecord grid : tenlist)
		{
			if (grid.getEci_GridCellMap().get(eci).getRsrp() >= (mr_rsrp - Range)
					&& grid.getEci_GridCellMap().get(eci).getRsrp() <= (mr_rsrp + Range))
			{
				suitTenGrididMap.put(new GridKey(grid.getIlongitude(), grid.getIlatitude(), grid.getLevel()), grid);
			}
		}
		return suitTenGrididMap;
	}

	/**
	 * 将40*40的格子按照主小区的rsrp分组
	 * 
	 * @param eci
	 * @return
	 */
	public void returnEci_rsrp_gridMap(long eci)
	{
		HashMap<Integer, HashMap<GridKey, FigureRecord>> eci_rsrp_gridMap = new HashMap<Integer, HashMap<GridKey, FigureRecord>>();
		HashMap<GridKey, FigureRecord> fortyGrididMap = figureData.get(40);
		if (fortyGrididMap == null)
		{
			this.eci_rsrp_gridMap = eci_rsrp_gridMap;
			return;
		}
		for (GridKey key : fortyGrididMap.keySet())
		{
			try
			{
				FigureRecord tempGrid = fortyGrididMap.get(key);
				int rsrp = (int) tempGrid.getEci_GridCellMap().get(eci).getRsrp();// rsrp转换成int类型
				HashMap<GridKey, FigureRecord> gridMap = eci_rsrp_gridMap.get(rsrp);
				if (gridMap == null)
				{
					gridMap = new HashMap<GridKey, FigureRecord>();
					eci_rsrp_gridMap.put(rsrp, gridMap);
				}
				gridMap.put(key, fortyGrididMap.get(key));
			}
			catch (Exception e)
			{
				continue;
			}
		}
		this.eci_rsrp_gridMap = eci_rsrp_gridMap;
	}

	/**
	 * 得到mr数据和各个栅格的比较结果
	 * 
	 * @param mrall
	 * @param result
	 *            mro和多个格子比较的结果
	 * @param gridMap
	 *            指纹栅格数据
	 */
	public void returnResultAndLocation(SIGNAL_MR_All mrall, ArrayList<OneGridResult> result,
			HashMap<GridKey, FigureRecord> gridMap)
	{
		int mrRsrp = mrall.tsc.LteScRSRP;
		int bigRsrp = mrRsrp + Range;
		int smallRsrp = mrRsrp - Range;
		long gongcanIlongitude = 0;
		long gongcanIlatitud = 0;
		ArrayList<Double> oneresult = null;
		FigureRecord grid = null;
		FigureCell gridcell = null;

		for (GridKey key : gridMap.keySet())// 每个栅格进行计算
		{
			oneresult = new ArrayList<Double>();
			grid = gridMap.get(key);
			gridcell = grid.getEci_GridCellMap().get((long) mrall.tsc.Eci);// mr主小区对应栅格
			if (((int) gridcell.getRsrp() >= smallRsrp) && ((int) gridcell.getRsrp() <= bigRsrp))
			{
				gongcanIlongitude = gridcell.getGongcanIlongitude();// 主小区公参经度
				gongcanIlatitud = gridcell.getGongcanIlatitud();// 主小区公参维度
				oneresult.add(Math.pow(mrRsrp - gridcell.getRsrp(), 2));// 主小区差值平方放第一位
				for (int j = 0; j < mrall.tlte.length; j++)// mr邻区
				{
					NC_LTE nclte = mrall.tlte[j];
					gridcell = grid.getEarfcn_pci_GridCellMap().get(nclte.LteNcEarfcn * 1000 + nclte.LteNcPci);
					if (gridcell != null)
					{
						oneresult.add(Math.pow(nclte.LteNcRSRP - gridcell.getRsrp(), 2));
					}
				}
			}
			if (oneresult.size() >= CellNum)// 至少cellnum个邻区匹配上
			{
				result.add(new OneGridResult(oneresult, grid.getLevel(), grid.getIlongitude(), grid.getIlatitude(),
						gongcanIlongitude, gongcanIlatitud, grid.getBuildingId()));
			}
		}
	}

	/**
	 * 对result结果进行预处理
	 * 
	 * @param result
	 */
	public void preResult(ArrayList<OneGridResult> result)
	{
		ArrayList<Double> tempresult = null;
		for (int i = 0; i < result.size(); i++)
		{
			tempresult = result.get(i).getOneresult();
			Object[] dd = tempresult.toArray();
			Arrays.sort(dd, 1, dd.length);// 对结果从第二个值开始排序。
			dd[0] = (double) dd[0] * 2;// 主小区 增加权重、扩大影响
			tempresult.clear();
			double sum = 0.0;
			for (int t = 0; t < (dd.length <= 4 ? dd.length : 4); t++)
			{
				sum += (double) dd[t];
				if (result.get(i).getLevel() == -1)
				{
					tempresult.add(Math.sqrt(sum + 1) / ((t + 1) * (t + 1))); // 直接保存用来比较的的结果
				}
				else
				{
					tempresult.add(Math.sqrt(sum + 1) / ((t + 1) * (t + 1) * pianyiNum)); // 直接保存用来比较的的结果
				}
			}
		}
		Collections.sort(result, new SortForListSize());// result按照oneresult.size排序
	}

	/**
	 * 找到result中最优值
	 * 
	 * @param result
	 * @return
	 */
	public OneGridResult getMrSimuLocation(ArrayList<OneGridResult> result)
	{
		if (result.size() == 1)
		{
			return result.get(0);
		}
		else
		{
			int best_Index = 0;
			int best_size = result.get(best_Index).getOneresult().size();
			for (int i = 1; i < result.size(); i++)
			{
				int current_size = result.get(i).getOneresult().size();
				if (current_size / 2 >= best_size)
				{
					best_Index = i;
					best_size = current_size;
				}
				else if (current_size == best_size)
				{
					if ((result.get(i).getOneresult().get(current_size - 1)
							- result.get(best_Index).getOneresult().get(best_size - 1) < 0)
							|| (result.get(i).getOneresult().get(current_size - 1)
									- result.get(best_Index).getOneresult().get(best_size - 1) == 0
									&& result.get(i).getLevel() >= 0 && result.get(best_Index).getLevel() < 0))// 差值相等也偏向cqt
					{
						best_Index = i;
						best_size = current_size;
					}
				}
				else
				{
					for (int j = best_size - 1; j < current_size; j++)
					{
						if (result.get(i).getOneresult().get(j)
								- result.get(best_Index).getOneresult().get(best_size - 1) <= 0)
						{
							best_Index = i;
							best_size = current_size;
							break;
						}
					}
				}
			}
			return result.get(best_Index);
		}
	}

	public void getIndoorSample(SIGNAL_MR_All mroItem, LteCellInfo cellInfo)
	{
		mroItem.tsc.longitude = cellInfo.ilongitude;
		mroItem.tsc.latitude = cellInfo.ilatitude;
		mroItem.loctp = "fl";
		mroItem.radius = 120;
		mroItem.imode = -2;// 室分定位标志
		mroItem.testType = StaticConfig.TestType_CQT;
	}

	/**
	 * 指纹库定位
	 * 
	 * @param mrall
	 * @return
	 */
	public boolean getSample(SIGNAL_MR_All mrall)
	{
		long eci = mrall.tsc.Eci;// 主小区的eci
		int mr_rsrp = mrall.tsc.LteScRSRP;// 主小区的场强
		ArrayList<OneGridResult> result = new ArrayList<OneGridResult>();// 装进计算后的结果。
		ArrayList<Integer> rsrplist = new ArrayList<Integer>();
		rsrplist.add(mr_rsrp);
		for (int i = 1; i <= Range; i++)
		{
			rsrplist.add(mr_rsrp + i);
			rsrplist.add(mr_rsrp - i);
		}
		for (int s : rsrplist)
		{
			HashMap<GridKey, FigureRecord> tempHashMap = eci_rsrp_gridMap.get(s);
			if (tempHashMap != null)
			{
				returnResultAndLocation(mrall, result, tempHashMap);// 得到mr数据和各个40*40栅格比较结果，以及各个栅格的经纬度
			}
		}
		if (result.size() > 0)
		{
			preResult(result);// 预处理结果result已经按照匹配到的小区数量进行升序排序了
			OneGridResult fixedGridResult = getMrSimuLocation(result);// 得到最优的栅格
			if (figureData.get(10) != null && figureData.get(10).size() > 0)
			{
				HashMap<GridKey, FigureRecord> suitTenGridMap = returnTenGridMap(fixedGridResult, mr_rsrp, eci);
				if (suitTenGridMap.size() > 0)
				{
					ArrayList<OneGridResult> tenResult = new ArrayList<OneGridResult>();
					returnResultAndLocation(mrall, tenResult, suitTenGridMap);// 得到mr数据和各个10*10栅格比较结果，以及各个栅格的经纬度
					if (tenResult.size() > 0)
					{
						preResult(tenResult);// 预处理结果result已经按照匹配到的小区数量进行升序排序了
						fixedGridResult = getMrSimuLocation(tenResult);// 得到最优的栅格
					}
				}
			}
			mrall.tsc.longitude = (int) fixedGridResult.getIlongitude();
			mrall.tsc.latitude = (int) fixedGridResult.getIlatitude();
			mrall.loctp = "fl";
			mrall.radius = 120;
			mrall.imode = (short) fixedGridResult.getLevel();// 指纹定位楼层高度
			mrall.ispeed = fixedGridResult.getBuildingId();// 指纹库定位楼宇id
			return true;
		}
		else
		{// mr没有找到符合要求的栅格
			return false;
		}
	}

	/**
	 * 将10*10的格子和40*40的格子映射
	 * 
	 * @return
	 */
	public void returnForty_tenGridMap()
	{
		HashMap<GridKey, FigureRecord> TenGrididMap = figureData.get(10);
		HashMap<GridKey, ArrayList<FigureRecord>> Forty_tenGridMap = new HashMap<GridKey, ArrayList<FigureRecord>>();
		if (TenGrididMap == null)
		{
			this.Forty_tenGridMap = Forty_tenGridMap;
			return;
		}
		for (GridKey key : TenGrididMap.keySet())
		{
			GridKey tempGridKey = new GridKey((key.getTllongitude() / 4000) * 4000 + 2000,
					(key.getTllatitude() / 3600) * 3600 + 1800, key.getLevel());
			ArrayList<FigureRecord> forty_ten_GridList = Forty_tenGridMap.get(tempGridKey);
			if (forty_ten_GridList == null)
			{
				forty_ten_GridList = new ArrayList<FigureRecord>();
				Forty_tenGridMap.put(tempGridKey, forty_ten_GridList);
			}
			forty_ten_GridList.add(TenGrididMap.get(key));
		}
		this.Forty_tenGridMap = Forty_tenGridMap;
	}

	/**
	 * 
	 * @param map
	 *            只有两个值 key=-1：表示dt//// key=1:表示cqt
	 * @param mergeMr
	 * @param level
	 * @return
	 */
	public HashMap<Long, HashMap<Integer, ArrayList<SIGNAL_MR_All>>> getMmeDtOrCqtMap(SIGNAL_MR_All mrItem)
	{
		int level = 0;
		if (mrItem.imode >= 0)
		{
			level = 1;
		}
		else
		{
			level = -1;
		}

		HashMap<Integer, ArrayList<SIGNAL_MR_All>> DtCqtMroItemMap = mmeues1apidDtCqtMap.get(mrItem.tsc.MmeUeS1apId);
		if (DtCqtMroItemMap == null)
		{
			DtCqtMroItemMap = new HashMap<Integer, ArrayList<SIGNAL_MR_All>>();
			ArrayList<SIGNAL_MR_All> MrItemList = new ArrayList<SIGNAL_MR_All>();
			MrItemList.add(mrItem);
			DtCqtMroItemMap.put(level, MrItemList);
			mmeues1apidDtCqtMap.put(mrItem.tsc.MmeUeS1apId, DtCqtMroItemMap);
		}
		else
		{
			ArrayList<SIGNAL_MR_All> MrItemList = DtCqtMroItemMap.get(level);
			if (MrItemList == null)
			{
				MrItemList = new ArrayList<SIGNAL_MR_All>();
				DtCqtMroItemMap.put(level, MrItemList);
			}
			MrItemList.add(mrItem);
		}
		return mmeues1apidDtCqtMap;
	}

	/*
	 * 赋值testType
	 */
	public void fillTestType(HashMap<Integer, ArrayList<SIGNAL_MR_All>> userSample, List<SIGNAL_MR_All> mroItemList,
			int testType)
	{
		if (userSample.get(1) != null)
		{
			for (SIGNAL_MR_All temp : userSample.get(1))
			{
				temp.testType = testType;
			}
			mroItemList.addAll(userSample.get(1));
		}
		if (userSample.get(-1) != null)
		{
			for (SIGNAL_MR_All temp : userSample.get(-1))
			{
				temp.testType = testType;
			}
			mroItemList.addAll(userSample.get(-1));
		}
	}

	/**
	 * 
	 * @param mroItemList
	 *            存储mro数据
	 * @param xdrmespidMap
	 *            xdr定位到的用户和testType
	 */
	public void ensureDtOrCqt(HashMap<Long, Integer> xdrmespidMap)
	{
		if (figureFixFlag != 1)
		{
			return;
		}
		for (long mmeues1apid : mmeues1apidDtCqtMap.keySet())
		{
			HashMap<Integer, ArrayList<SIGNAL_MR_All>> userSample = mmeues1apidDtCqtMap.get(mmeues1apid);
			if (xdrmespidMap.containsKey(mmeues1apid))
			{
				int testType = xdrmespidMap.get(mmeues1apid);
				fillTestType(userSample, FigureMroItemList, testType);
			}
			else
			{
				int cqtsize = 0;
				int dtsize = 0;
				if (userSample.get(1) == null)
				{
					cqtsize = 0;
				}
				else
				{
					cqtsize = userSample.get(1).size();
				}
				if (userSample.get(-1) == null)
				{
					dtsize = 0;
				}
				else
				{
					dtsize = userSample.get(-1).size();
				}
				if (cqtsize / (double) (dtsize + cqtsize) >= percent)
				{
					fillTestType(userSample, FigureMroItemList, StaticConfig.TestType_CQT);
				}
				else if (dtsize / (double) (dtsize + cqtsize) >= percent)
				{
					fillTestType(userSample, FigureMroItemList, StaticConfig.TestType_DT);
				}
				else
				{
					fillTestType(userSample, FigureMroItemList, StaticConfig.TestType_DT_EX);
				}
			}
		}
	}

	public void setup() throws IOException, InterruptedException
	{
		MainModel.GetInstance().setConf(conf);

		////////////////////// 初始化输出控制指纹库定位
		fpath_sample = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample");
		fpath_event = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_event");
		fpath_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cell");
		fpath_cell_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cell_freq");
		fpath_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_cellgrid");
		fpath_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid");
		fpath_ImsiSampleIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ImsiSampleIndex");
		fpath_ImsiEventIndex = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ImsiEventIndex");
		fpath_myLog = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_myLog");
		fpath_locMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_locMore");
		fpath_mroMore = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_mroMore");

		fpath_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_dt");
		fpath_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_dt_freq");
		fpath_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_cqt");
		fpath_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_grid_cqt_freq");
		fpath_sample_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_dt");
		fpath_sample_dtex = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_dtex");
		fpath_sample_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_cqt");
		fpath_sample_index_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_index_dt");
		fpath_sample_index_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_sample_index_cqt");

		fpath_ten_grid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid");
		fpath_ten_grid_dt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_dt");
		fpath_ten_grid_dt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_dt_freq");
		fpath_ten_grid_cqt = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_cqt");
		fpath_ten_grid_cqt_freq = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_grid_cqt_freq");
		fpath_ten_cellgrid = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_ten_cellgrid");

		fpath_useract_cell = conf.get("mastercom.mroxdrmerge.mro.locfillex.fpath_useract_cell");

		// 初始化输出控制
		mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());

		////////////////////// 初始化输出控制指纹库定位

		mosMng.SetOutputPath("mrosample", fpath_sample);
		mosMng.SetOutputPath("mroevent", fpath_event);
		mosMng.SetOutputPath("mrocell", fpath_cell);
		mosMng.SetOutputPath("mrocellfreq", fpath_cell_freq);
		mosMng.SetOutputPath("mrocellgrid", fpath_cellgrid);
		mosMng.SetOutputPath("mrogrid", fpath_grid);
		mosMng.SetOutputPath("imsisampleindex", fpath_ImsiSampleIndex);
		mosMng.SetOutputPath("imsieventindex", fpath_ImsiEventIndex);
		mosMng.SetOutputPath("myLog", fpath_myLog);
		mosMng.SetOutputPath("locMore", fpath_locMore);
		mosMng.SetOutputPath("mroMore", fpath_mroMore);
		mosMng.SetOutputPath("griddt", fpath_grid_dt);
		mosMng.SetOutputPath("griddtfreq", fpath_grid_dt_freq);
		mosMng.SetOutputPath("gridcqt", fpath_grid_cqt);
		mosMng.SetOutputPath("gridcqtfreq", fpath_grid_cqt_freq);
		mosMng.SetOutputPath("sampledt", fpath_sample_dt);
		mosMng.SetOutputPath("sampledtex", fpath_sample_dtex);
		mosMng.SetOutputPath("samplecqt", fpath_sample_cqt);
		mosMng.SetOutputPath("sampleindexdt", fpath_sample_index_dt);
		mosMng.SetOutputPath("sampleindexcqt", fpath_sample_index_cqt);
		mosMng.SetOutputPath("useractcell", fpath_useract_cell);

		mosMng.SetOutputPath("tenmrogrid", fpath_ten_grid);
		mosMng.SetOutputPath("tengriddt", fpath_ten_grid_dt);
		mosMng.SetOutputPath("tengriddtfreq", fpath_ten_grid_dt_freq);
		mosMng.SetOutputPath("tengridcqt", fpath_ten_grid_cqt);
		mosMng.SetOutputPath("tengridcqtfreq", fpath_ten_grid_cqt_freq);
		mosMng.SetOutputPath("tenmrocellgrid", fpath_ten_cellgrid);
		mosMng.init();
		// 初始化小区的信息
		if (!CellConfig.GetInstance().loadLteCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
			throw (new IOException("cellconfig init error 请检查！" + CellConfig.GetInstance().errLog));
		}

		statDeal = new StatDeal(mosMng);
		statDeal_DT = new StatDeal_DT(mosMng);
		statDeal_CQT = new StatDeal_CQT(mosMng);
		userActStatMng = new UserActStatMng();

		// 打印状态日志
		LOGHelper.GetLogger().writeLog(LogType.info,
				"cellconfig init count is : " + CellConfig.GetInstance().getLteCellInfoMap().size());
	}

	public void cleanup() throws IOException, InterruptedException
	{
		try
		{
			outUserData();
			outAllData();
		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "output data error ", e);
		}
		mosMng.close();
	}

	// 吐出用户过程数据，为了防止内存过多
	public void outDealingData()
	{
		if (figureFixFlag != 1)
		{
			return;
		}
		dealSample();

		// 天数据吐出/////////////////////////////////////////////////////////////////////////////////////
		statDeal.outDealingData();
		statDeal_DT.outDealingData();
		statDeal_CQT.outDealingData();

		// 如果用户数据大于10000个，就吐出去先
		if (userActStatMng.getUserActStatMap().size() > 10000)
		{
			userActStatMng.finalStat();

			// 用户行动信息输出
			for (UserActStat userActStat : userActStatMng.getUserActStatMap().values())
			{
				try
				{
					StringBuffer sb = new StringBuffer();
					String TabMark = "\t";
					for (UserActTime userActTime : userActStat.userActTimeMap.values())
					{
						for (UserCellAll userActAll : userActTime.userCellAllMap.values())
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
							UserCell mainUserCell = userActAll.getMainUserCell();
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
							List<UserCell> userCellList = userActAll.getUserCellList();
							int sn = 1;
							for (UserCell userCell : userCellList)
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
					LOGHelper.GetLogger().writeLog(LogType.error, "user action error", e);
				}
			}

			userActStatMng = new UserActStatMng();
		}

	}

	// 将会吐出用户最后所有数据
	private void outUserData()
	{

	}

	private void outAllData()
	{
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
				for (UserActTime userActTime : userActStat.userActTimeMap.values())
				{
					for (UserCellAll userActAll : userActTime.userCellAllMap.values())
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
						UserCell mainUserCell = userActAll.getMainUserCell();
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
						List<UserCell> userCellList = userActAll.getUserCellList();
						int sn = 1;
						for (UserCell userCell : userCellList)
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
				LOGHelper.GetLogger().writeLog(LogType.error, "user action error", e);
			}
		}

	}

	private void dealSample()
	{
		DT_Sample_4G sample = new DT_Sample_4G();
		int dist;
		int maxRadius = 6000;

		for (SIGNAL_MR_All data : FigureMroItemList)
		{
			sample.Clear();

			// 如果采样点过远就需要筛除
			LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(data.tsc.Eci);
			dist = -1;
			if (lteCellInfo != null)
			{
				if (data.tsc.longitude > 0 && data.tsc.latitude > 0 && lteCellInfo.ilongitude > 0
						&& lteCellInfo.ilatitude > 0)
				{
					dist = (int) GisFunction.GetDistance(data.tsc.longitude, data.tsc.latitude, lteCellInfo.ilongitude,
							lteCellInfo.ilatitude);
				}
			}
			data.dist = dist;
			if (dist < 0 || dist > maxRadius)
			{
				continue;
			}

			// 基于Ta进行筛
			if (data.tsc.LteScTadv >= 15 && data.tsc.LteScTadv < 1282)
			{
				double taDist = MrLocation.calcDist(data.tsc.LteScTadv, data.tsc.LteScRTTD);
				if (dist > taDist * 1.2)
				{
					data.dist = -1;
					data.tsc.longitude = 0;
					data.tsc.latitude = 0;
					data.testType = StaticConfig.TestType_OTHER;
				}
			}

			statMro(sample, data);
			statKpi(sample);
		}
	}

	private void statKpi(DT_Sample_4G sample)
	{
		// cpe不参与kpi运算
		if (sample.testType == StaticConfig.TestType_CPE)
		{
			return;
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

	}

	private void statMro(DT_Sample_4G tsam, SIGNAL_MR_All tTemp)
	{
		tsam.ispeed = tTemp.ispeed; // 标识经纬度来源
		tsam.imode = (short) tTemp.imode;// 标识楼层高度
		tsam.cityID = tTemp.tsc.cityID;
		tsam.itime = tTemp.tsc.beginTime;
		tsam.wtimems = (short) (tTemp.tsc.beginTimems);
		tsam.ilongitude = tTemp.tsc.longitude;
		tsam.ilatitude = tTemp.tsc.latitude;
		tsam.IMSI = tTemp.tsc.IMSI;
		tsam.iLAC = (int) getValidData(tsam.iLAC, tTemp.tsc.TAC);
		tsam.iCI = (long) getValidData(tsam.iCI, tTemp.tsc.CellId);
		tsam.Eci = (long) getValidData(tsam.Eci, tTemp.tsc.Eci);
		tsam.eventType = 0;
		tsam.ENBId = (int) getValidData(tsam.ENBId, tTemp.tsc.ENBId);
		tsam.UserLabel = tTemp.tsc.UserLabel;
		tsam.CellId = (long) getValidData(tsam.CellId, tTemp.tsc.CellId);
		tsam.Earfcn = tTemp.tsc.Earfcn;
		tsam.SubFrameNbr = tTemp.tsc.SubFrameNbr;
		tsam.MmeCode = (int) getValidData(tsam.MmeCode, tTemp.tsc.MmeCode);
		tsam.MmeGroupId = (int) getValidData(tsam.MmeGroupId, tTemp.tsc.MmeGroupId);
		tsam.MmeUeS1apId = (long) getValidData(tsam.MmeUeS1apId, tTemp.tsc.MmeUeS1apId);
		tsam.Weight = tTemp.tsc.Weight;
		tsam.LteScRSRP = tTemp.tsc.LteScRSRP;
		tsam.LteScRSRQ = tTemp.tsc.LteScRSRQ;
		tsam.LteScEarfcn = tTemp.tsc.LteScEarfcn;
		tsam.LteScPci = tTemp.tsc.LteScPci;
		tsam.LteScBSR = tTemp.tsc.LteScBSR;
		tsam.LteScRTTD = tTemp.tsc.LteScRTTD;
		tsam.LteScTadv = tTemp.tsc.LteScTadv;
		tsam.LteScAOA = tTemp.tsc.LteScAOA;
		tsam.LteScPHR = tTemp.tsc.LteScPHR;
		tsam.LteScRIP = tTemp.tsc.LteScRIP;
		tsam.LteScSinrUL = tTemp.tsc.LteScSinrUL;
		tsam.LocFillType = 1;

		tsam.testType = tTemp.testType;
		tsam.location = tTemp.location;
		tsam.dist = tTemp.dist;
		tsam.radius = tTemp.radius;
		tsam.loctp = tTemp.loctp;
		tsam.indoor = tTemp.indoor;
		tsam.networktype = tTemp.networktype;
		tsam.lable = tTemp.lable;

		tsam.serviceType = tTemp.serviceType;
		tsam.serviceSubType = tTemp.subServiceType;

		tsam.moveDirect = tTemp.moveDirect;

		tsam.LteScPUSCHPRBNum = tTemp.tsc.LteScPUSCHPRBNum;
		tsam.LteScPDSCHPRBNum = tTemp.tsc.LteScPDSCHPRBNum;
		tsam.LteSceNBRxTxTimeDiff = tTemp.tsc.LteSceNBRxTxTimeDiff;

		if (tTemp.tsc.EventType.length() > 0)
		{
			if (tTemp.tsc.EventType.equals("MRO"))
			{
				tsam.flag = "MRO";
			}
			else
			{
				tsam.flag = "MRE";
				tsam.mrType = tTemp.tsc.EventType;
			}
		}
		else
		{
			tsam.flag = "MRO";
			int mrTypeIndex = tTemp.tsc.UserLabel.indexOf(",");
			if (mrTypeIndex >= 0)
			{
				tsam.flag = "MRE";
				tsam.mrType = tTemp.tsc.UserLabel.substring(mrTypeIndex + 1);
			}
		}

		for (int i = 0; i < tsam.nccount.length; i++)
		{
			tsam.nccount[i] = tTemp.nccount[i];
		}

		for (int i = 0; i < tsam.tlte.length; i++)
		{
			tsam.tlte[i] = tTemp.tlte[i];
		}

		for (int i = 0; i < tsam.ttds.length; i++)
		{
			tsam.ttds[i] = tTemp.ttds[i];
		}

		for (int i = 0; i < tsam.tgsm.length; i++)
		{
			tsam.tgsm[i] = tTemp.tgsm[i];
		}

		for (int i = 0; i < tsam.trip.length; i++)
		{
			tsam.trip[i] = tTemp.trip[i];
		}

		calJamType(tsam);

		// output to hbase
		try
		{
			// 只输出哈尔滨
			// if(tsam.cityID == 3)
			// {
			// curText.set(ResultHelper.getPutLteSample(tsam));
			// mosMng.write("mrosample", NullWritable.get(), curText);
			// }

			if (tsam.testType == StaticConfig.TestType_DT)
			{
				if (tsam.ilongitude > 0)
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("sampledt", NullWritable.get(), curText);
				}
			}
			else if (tsam.testType == StaticConfig.TestType_DT_EX || tsam.testType == StaticConfig.TestType_CPE)
			{
				if (tsam.ilongitude > 0)
				{
					curText.set(ResultHelper.getPutLteSample(tsam));
					mosMng.write("sampledtex", NullWritable.get(), curText);
				}
			}
			else if (tsam.testType == StaticConfig.TestType_CQT)
			{
				curText.set(ResultHelper.getPutLteSample(tsam));
				mosMng.write("samplecqt", NullWritable.get(), curText);
			}

			if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
			{
				// 吐出关联的中间结果
				tmSb.delete(0, tmSb.length());
				tmSb.append(tsam.Eci + "_" + tsam.MmeUeS1apId + "_" + tsam.itime);
				tmSb.append("\t");
				tmSb.append(tsam.Earfcn);
				tmSb.append("_");
				tmSb.append(tsam.LteScPci);
				tmSb.append("_");
				tmSb.append(tsam.LteScRSRP);
				tmSb.append("_");
				tmSb.append(tsam.IMSI);
				tmSb.append("_");
				tmSb.append(tsam.ilongitude);
				tmSb.append("_");
				tmSb.append(tsam.ilatitude);

				curText.set(tmSb.toString());
				mosMng.write("mroMore", NullWritable.get(), curText);
			}

		}
		catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "output event error ", e);
			// TODO: handle exception
		}

	}

	public void calJamType(DT_Sample_4G tsam)
	{
		if ((tsam.LteScRSRP < -50 && tsam.LteScRSRP > -150) && tsam.LteScRSRP > -110)
		{
			for (NC_LTE item : tsam.tlte)
			{
				if ((item.LteNcRSRP < -50 && item.LteNcRSRP > -150) && item.LteNcRSRP - tsam.LteScRSRP > -6)
				{
					if (tsam.Earfcn == item.LteNcEarfcn)
					{
						tsam.sfcnJamCellCount++;
					}
					else
					{
						tsam.dfcnJamCellCount++;
					}
				}
			}
		}
	}

	private Object getValidData(Object srcData, Object tarData)
	{
		if (tarData instanceof Integer)
		{
			if ((Integer) tarData != 0 && (Integer) tarData != StaticConfig.Int_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		else if (tarData instanceof Long)
		{
			if ((Long) tarData != 0 && (Long) tarData != StaticConfig.Long_Abnormal)
			{
				return tarData;
			}
			return srcData;
		}
		return srcData;
	}

	public int getValidValueInt(int srcValue, int targValue)
	{
		if (targValue != StaticConfig.Int_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

	public String getValidValueString(String srcValue, String targValue)
	{
		if (!targValue.equals(""))
		{
			return targValue;
		}
		return srcValue;
	}

	public long getValidValueLong(long srcValue, long targValue)
	{
		if (targValue != StaticConfig.Long_Abnormal)
		{
			return targValue;
		}
		return srcValue;
	}

}
