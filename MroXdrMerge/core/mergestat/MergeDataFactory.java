package mergestat;

import java.util.HashMap;
import java.util.Map;

import mdtstat.mergeBySize.MdtInCellGrid_mergeBySize;
import mdtstat.mergeBySize.MdtInGrid_mergeBySize;
import mdtstat.mergeBySize.MdtOutCellGrid_mergeBySize;
import mdtstat.struct.MdtBuildCell_mergeDo;
import mdtstat.struct.MdtBuild_mergeDo;
import mdtstat.struct.MdtCell_mergeDo;
import mdtstat.struct.MdtImei_mergeDo;
import mdtstat.struct.MdtInCellGrid_mergeDo;
import mdtstat.struct.MdtInGrid_mergeDo;
import mdtstat.struct.MdtOutCellGrid_mergeDo;
import mdtstat.struct.MdtOutGrid_mergeDo;
import mro.lablefill.UserActCellMergeDataDo;
import mrstat.mergeBySize.InCellGridMergeBySize;
import mrstat.mergeBySize.InGridMergeBySize;
import mrstat.mergeBySize.OutCellGridMergeBySize;
import mrstat.mergeBySize.OutGridMergeBySize;
import mrstat.struct.BuildCellMergeDo;
import mrstat.struct.BuildMergeDo;
import mrstat.struct.CellMergeDo;
import mrstat.struct.InCellGridMergeDo;
import mrstat.struct.InGridMergeDo;
import mrstat.struct.OutCellGridMergeDo;
import mrstat.struct.OutGridMerge;
import mrstat.struct.SceneCellGridMergeDo;
import mrstat.struct.SceneCellMergeDo;
import mrstat.struct.SceneGridMergeDo;
import mrstat.struct.SceneMergeDo;
import xdr.lablefill.CellGridMergeDataDo_4G;
import xdr.lablefill.CellMergeDataDo_4G;
import xdr.lablefill.CellMergeDataDo_Freq;
import xdr.lablefill.FreqCellByImeiDataMergeDo;
import xdr.lablefill.GridMergeDataDo_4G;
import xdr.lablefill.GridMergeFreqByImeiDataDo_4G;
import xdr.lablefill.GridMergeFreqDataDo_4G;
import xdr.lablefill.UserGridMergeDataDo_4G;
import xdr.lablefill.by23g.CellGridMergeDataDo_2G;
import xdr.lablefill.by23g.CellGridMergeDataDo_3G;
import xdr.lablefill.by23g.CellMergeDataDo_2G;
import xdr.lablefill.by23g.CellMergeDataDo_3G;
import xdr.lablefill.by23g.GridMergeDataDo_2G;
import xdr.lablefill.by23g.GridMergeDataDo_3G;
import xdr.locallex.struct.EventAreaCellGrid_mergeDo;
import xdr.locallex.struct.EventAreaCell_mergeDo;
import xdr.locallex.struct.EventAreaGrid_mergeDo;
import xdr.locallex.struct.EventArea_mergeDo;
import xdr.locallex.struct.EventBuildCellGrid_mergeDo;
import xdr.locallex.struct.EventBuildGrid_mergeDo;
import xdr.locallex.struct.EventCell_mergeDo;
import xdr.locallex.struct.EventInCellGrid_mergeDo;
import xdr.locallex.struct.EventInGrid_mergeDo;
import xdr.locallex.struct.EventOutCellGrid_mergeDo;
import xdr.locallex.struct.EventOutGrid_mergeDo;

public class MergeDataFactory
{
	public static int MERGETYPE_CELLSTAT_2G = 1;
	public static int MERGETYPE_CELLSTAT_3G = 2;
	public static int MERGETYPE_GRIDSTAT_2G = 3;
	public static int MERGETYPE_GRIDSTAT_3G = 4;
	public static int MERGETYPE_CELLGRIDSTAT_2G = 5;
	public static int MERGETYPE_CELLGRIDSTAT_3G = 6;

	public static int MERGETYPE_GRIDSTAT_DT_2G = 7;
	public static int MERGETYPE_GRIDSTAT_DT_3G = 8;
	public static int MERGETYPE_CELLGRIDSTAT_DT_2G = 9;
	public static int MERGETYPE_CELLGRIDSTAT_DT_3G = 10;

	public static int MERGETYPE_GRIDSTAT_CQT_2G = 11;
	public static int MERGETYPE_GRIDSTAT_CQT_3G = 12;
	public static int MERGETYPE_CELLGRIDSTAT_CQT_2G = 13;
	public static int MERGETYPE_CELLGRIDSTAT_CQT_3G = 14;

	public static int MERGETYPE_GRIDSTAT_DT_4G = 15;
	public static int MERGETYPE_GRIDSTAT_CQT_4G = 16;
	public static int MERGETYPE_GRIDSTAT_ALL_4G = 17;

	public static int MERGETYPE_CELLSTAT_4G = 18;

	public static int MERGETYPE_CELLGRIDSTAT_4G = 19;

	public static int MERGETYPE_USERGRIDSTAT_4G = 20;

	public static int MERGETYPE_GRIDSTAT_DT_4G_FREQ = 21;

	public static int MERGETYPE_GRIDSTAT_CQT_4G_FREQ = 22;

	public static int MERGETYPE_CELLSTAT_FREQ = 23;

	public static int MERGETYPE_USERACT_CELL = 24;

	public static int MERGETYPE_FIGURE_GRID_4G = 25;

	public static int MERGETYPE_FIGURE_DTGRID_4G = 26;

	public static int MERGETYPE_FIGURE_CQTGRID_4G = 27;

	public static int MERGETYPE_FIGURE_CELLGRID_4G = 28;

	public static int MERGETYPE_FIGURE_FREQ_DTGRID_4G = 29;

	public static int MERGETYPE_FIGURE_FREQ_CQTGRID_4G = 30;

	public static int MERGETYPE_FIGURE_GRID_4G_10 = 31;

	public static int MERGETYPE_FIGURE_DTGRID_4G_10 = 32;

	public static int MERGETYPE_FIGURE_CQTGRID_4G_10 = 33;

	public static int MERGETYPE_FIGURE_CELLGRID_4G_10 = 34;

	public static int MERGETYPE_FIGURE_FREQ_DTGRID_4G_10 = 35;

	public static int MERGETYPE_FIGURE_FREQ_CQTGRID_4G_10 = 36;

	public static int MERGETYPE_GRID_4G_10 = 37;

	public static int MERGETYPE_DTGRID_4G_10 = 38;

	public static int MERGETYPE_CQTGRID_4G_10 = 39;

	public static int MERGETYPE_CELLGRID_4G_10 = 40;

	public static int MERGETYPE_FREQ_DTGRID_4G_10 = 41;

	public static int MERGETYPE_FREQ_CQTGRID_4G_10 = 42;

	// 新添加统计 freq grid byImei

	public static int MERGETYPE_FREQ_GRID_BYIMEI_DT = 43;
	public static int MERGETYPE_FREQ_GRID_BYIMEI_DT_10 = 44;
	public static int MERGETYPE_FREQ_GRID_BYIMEI_CQT = 45;
	public static int MERGETYPE_FREQ_GRID_BYIMEI_CQT_10 = 46;

	public static int MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT = 47;
	public static int MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT_10 = 48;
	public static int MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT = 49;
	public static int MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT_10 = 50;

	// 新添加dt、cqt cellgrid
	public static int MERGETYPE_CELL_GRID_DT = 51;
	public static int MERGETYPE_CELL_GRID_CQT = 52;
	public static int MERGETYPE_CELL_GRID_DT_10 = 53;
	public static int MERGETYPE_CELL_GRID_CQT_10 = 54;

	public static int MERGETYPE_FIGURE_CELL_GRID_DT = 55;
	public static int MERGETYPE_FIGURE_CELL_GRID_CQT = 56;
	public static int MERGETYPE_FIGURE_CELL_GRID_DT_10 = 57;
	public static int MERGETYPE_FIGURE_CELL_GRID_CQT_10 = 58;

	// 新添加统计 freq grid byImei 电信
	public static int MERGETYPE_DX_FREQ_GRID_BYIMEI_DT_10 = 59;
	public static int MERGETYPE_DX_FREQ_GRID_BYIMEI_CQT_10 = 60;
	public static int MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_DT_10 = 61;
	public static int MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_CQT_10 = 62;
	// freqCell by imei
	public static int MERGETYPE_LT_CELL_BYIMEI = 63;
	public static int MERGETYPE_DX_CELL_BYIMEI = 64;
	public static int MERGETYPE_LT_FIGURE_CELL_BYIMEI = 65;
	public static int MERGETYPE_DX_FIGURE_CELL_BYIMEI = 66;

	// 新表汇聚 2017.06.20

	public static int MERGETYPE_HIGH_OUT_GRID = 67;
	public static int MERGETYPE_MID_OUT_GRID = 68;
	public static int MERGETYPE_LOW_OUT_GRID = 69;

	public static int MERGETYPE_HIGH_OUT_GRID_CELL = 70;
	public static int MERGETYPE_MID_OUT_GRID_CELL = 71;
	public static int MERGETYPE_LOW_OUT_GRID_CELL = 72;

	public static int MERGETYPE_HIGH_IN_GRID = 73;
	public static int MERGETYPE_MID_IN_GRID = 74;
	public static int MERGETYPE_LOW_IN_GRID = 75;

	public static int MERGETYPE_HIGH_IN_GRID_CELL = 76;
	public static int MERGETYPE_MID_IN_GRID_CELL = 77;
	public static int MERGETYPE_LOW_IN_GRID_CELL = 78;

	public static int MERGETYPE_HIGH_BUILD = 79;
	public static int MERGETYPE_MID_BUILD = 80;
	public static int MERGETYPE_LOW_BUILD = 81;

	public static int MERGETYPE_CELL = 82;

	public static int MERGETYPE_DX_HIGH_OUT_GRID = 83;
	public static int MERGETYPE_DX_MID_OUT_GRID = 84;
	public static int MERGETYPE_DX_LOW_OUT_GRID = 85;

	public static int MERGETYPE_DX_HIGH_IN_GRID = 86;
	public static int MERGETYPE_DX_MID_IN_GRID = 87;
	public static int MERGETYPE_DX_LOW_IN_GRID = 88;

	public static int MERGETYPE_DX_HIGH_BUILD = 89;
	public static int MERGETYPE_DX_MID_BUILD = 90;
	public static int MERGETYPE_DX_LOW_BUILD = 91;

	public static int MERGETYPE_DX_CELL = 92;

	public static int MERGETYPE_LT_HIGH_OUT_GRID = 93;
	public static int MERGETYPE_LT_MID_OUT_GRID = 94;
	public static int MERGETYPE_LT_LOW_OUT_GRID = 95;

	public static int MERGETYPE_LT_HIGH_IN_GRID = 96;
	public static int MERGETYPE_LT_MID_IN_GRID = 97;
	public static int MERGETYPE_LT_LOW_IN_GRID = 98;

	public static int MERGETYPE_LT_HIGH_BUILD = 99;
	public static int MERGETYPE_LT_MID_BUILD = 100;
	public static int MERGETYPE_LT_LOW_BUILD = 101;

	public static int MERGETYPE_LT_CELL = 102;

	// AREA
	public static int AREA_CELL = 103;
	public static int AREA_CELL_GRID = 104;
	public static int AREA_GRID = 105;
	public static int AREA = 106;

	// MDT
	public static int TB_MDTMR_BUILD_HIGH = 107;
	public static int TB_MDTMR_BUILD_LOW = 108;

	public static int TB_MDTMR_OUTGRID_HIGH = 109;
	public static int TB_MDTMR_OUTGRID_LOW = 110;

	public static int TB_MDTMR_INGRID_HIGH = 111;
	public static int TB_MDTMR_INGRID_LOW = 112;

	public static int TB_MDTMR_INGRID_CELL_HIGH = 113;
	public static int TB_MDTMR_INGRID_CELL_LOW = 114;

	public static int TB_MDTMR_OUTGRID_CELL_HIGH = 115;
	public static int TB_MDTMR_OUTGRID_CELL_LOW = 116;

	public static int TB_MDTMR_CELL = 117;
	public static int TB_MDT_IMEI = 118;

	public static int TB_LT_MDTMR_BUILD_HIGH = 119;
	public static int TB_LT_MDTMR_BUILD_LOW = 120;

	public static int TB_LT_MDTMR_OUTGRID_HIGH = 121;
	public static int TB_LT_MDTMR_OUTGRID_LOW = 122;

	public static int TB_LT_MDTMR_INGRID_HIGH = 123;
	public static int TB_LT_MDTMR_INGRID_LOW = 124;

	public static int TB_LT_MDTMR_CELL = 125;

	public static int TB_DX_MDTMR_BUILD_HIGH = 126;
	public static int TB_DX_MDTMR_BUILD_LOW = 127;

	public static int TB_DX_MDTMR_OUTGRID_HIGH = 128;
	public static int TB_DX_MDTMR_OUTGRID_LOW = 129;

	public static int TB_DX_MDTMR_INGRID_HIGH = 130;
	public static int TB_DX_MDTMR_INGRID_LOW = 131;

	public static int TB_DX_MDTMR_CELL = 132;

	public static int TB_MDTMR_BUILDCELL_HIGH = 133;
	public static int TB_MDTMR_BUILDCELL_LOW = 134;
	// 8.31add new mro table ,buildcell
	public static int TB_MR_CELLBUILD_HIGH = 135;
	public static int TB_MR_CELLBUILD_MID = 136;
	public static int TB_MR_CELLBUILD_LOW = 137;
	
	//mdt round grid bigger 9.12
	public static int MDT_ROUND_YD_OUTGRID_HIGH = 138;
	public static int MDT_ROUND_YD_OUTGRID_LOW = 139;
	public static int MDT_ROUND_YD_INGRID_HIGH = 140;
	public static int MDT_ROUND_YD_INGRID_LOW = 141;
	public static int MDT_ROUND_INGRID_CELL_HIGH = 142;
	public static int MDT_ROUND_INGRID_CELL_LOW = 143;
	public static int MDT_ROUND_OUTGRID_CELL_HIGH = 144;
	public static int MDT_ROUND_OUTGRID_CELL_LOW = 145;
	public static int MDT_ROUND_LT_OUTGRID_HIGH = 146;
	public static int MDT_ROUND_LT_OUTGRID_LOW = 147;
	public static int MDT_ROUND_LT_INGRID_HIGH = 148;
	public static int MDT_ROUND_LT_INGRID_LOW = 149;
	public static int MDT_ROUND_DX_OUTGRID_HIGH = 150;
	public static int MDT_ROUND_DX_OUTGRID_LOW = 151;
	public static int MDT_ROUND_DX_INGRID_HIGH = 152;
	public static int MDT_ROUND_DX_INGRID_LOW = 153;
	
	//mro round grid bigger 9.12
	public static int MRO_ROUND_YD_OUTGRID_HIGH = 154;
	public static int MRO_ROUND_YD_OUTGRID_MID = 155;
	public static int MRO_ROUND_YD_OUTGRID_LOW = 156;
	public static int MRO_ROUND_OUTGRID_CELL_HIGH = 157;
	public static int MRO_ROUND_OUTGRID_CELL_MID = 158;
	public static int MRO_ROUND_OUTGRID_CELL_LOW = 159;
	public static int MRO_ROUND_YD_INGRID_HIGH = 160;
	public static int MRO_ROUND_YD_INGRID_MID = 161;
	public static int MRO_ROUND_YD_INGRID_LOW = 162;
	public static int MRO_ROUND_INGRID_CELL_HIGH = 163;
	public static int MRO_ROUND_INGRID_CELL_MID = 164;
	public static int MRO_ROUND_INGRID_CELL_LOW = 165;
	public static int MRO_ROUND_DX_OUTGRID_HIGH = 166;
	public static int MRO_ROUND_DX_OUTGRID_MID = 167;
	public static int MRO_ROUND_DX_OUTGRID_LOW = 168;
	public static int MRO_ROUND_DX_INGRID_HIGH = 169;
	public static int MRO_ROUND_DX_INGRID_MID = 170;
	public static int MRO_ROUND_DX_INGRID_LOW = 171;
	public static int MRO_ROUND_LT_OUTGRID_HIGH = 172;
	public static int MRO_ROUND_LT_OUTGRID_MID = 173;
	public static int MRO_ROUND_LT_OUTGRID_LOW = 174;
	public static int MRO_ROUND_LT_INGRID_HIGH = 175;
	public static int MRO_ROUND_LT_INGRID_MID = 176;
	public static int MRO_ROUND_LT_INGRID_LOW = 177;
	
	// yzx add LT and DX AREA 2017.9.18
	public static int TB_LT_AREA_CELL = 178;
	public static int TB_LT_AREA_GRID = 180;
	public static int TB_LT_AREA = 181;
	
	public static int TB_DX_AREA_CELL = 182;
	public static int TB_DX_AREA_GRID = 184;
	public static int TB_DX_AREA = 185;
	
	//yzx add EVENT 2017.9.26
	public static int TB_EVENT_CELL = 186;
	public static int TB_EVENT_INGRID_CELL_HIGH = 187;
	public static int TB_EVENT_INGRID_CELL_MID = 188;
	public static int TB_EVENT_INGRID_CELL_LOW = 189;
	public static int TB_EVENT_INGRID_HIGH = 190;
	public static int TB_EVENT_INGRID_MID = 191;
	public static int TB_EVENT_INGRID_LOW = 192;
	public static int TB_EVENT_OUTGRID_CELL_HIGH = 193;
	public static int TB_EVENT_OUTGRID_CELL_MID = 194;
	public static int TB_EVENT_OUTGRID_CELL_LOW = 195;
	public static int TB_EVENT_OUTGRID_HIGH = 196;
	public static int TB_EVENT_OUTGRID_MID = 197;
	public static int TB_EVENT_OUTGRID_LOW = 198;
	public static int TB_EVENT_BUILD_GRID_CELL_HIGH = 199;
	public static int TB_EVENT_BUILD_GRID_CELL_MID = 200;
	public static int TB_EVENT_BUILD_GRID_CELL_LOW = 201;
	public static int TB_EVENT_BUILD_GRID_HIGH = 202;
	public static int TB_EVENT_BUILD_GRID_MID = 203;
	public static int TB_EVENT_BUILD_GRID_LOW = 204;
	
	public static int TB_EVENT_AREA = 205;
	public static int TB_EVENT_AREA_GRID = 206;
	public static int TB_EVENT_AREA_GRID_CELL = 207;
	public static int TB_EVENT_AREA_CELL = 208;
	
	//20170921 add fullnet
	public static int MERGETYPE_YDLT_OUTGRID_HIGH = 209;
	public static int MERGETYPE_YDLT_OUTGRID_MID = 210;
	public static int MERGETYPE_YDLT_OUTGRID_LOW = 211;
	public static int MERGETYPE_YDDX_OUTGRID_HIGH = 212;
	public static int MERGETYPE_YDDX_OUTGRID_MID = 213;
	public static int MERGETYPE_YDDX_OUTGRID_LOW = 214;
	public static int MERGETYPE_YDLT_INGRID_HIGH = 215;
	public static int MERGETYPE_YDLT_INGRID_MID = 216;
	public static int MERGETYPE_YDLT_INGRID_LOW = 217;
	public static int MERGETYPE_YDDX_INGRID_HIGH = 218;
	public static int MERGETYPE_YDDX_INGRID_MID = 219;
	public static int MERGETYPE_YDDX_INGRID_LOW = 220;
	public static int MERGETYPE_YDLT_BUILDING_HIGH = 221;
	public static int MERGETYPE_YDLT_BUILDING_MID = 222;
	public static int MERGETYPE_YDLT_BUILDING_LOW = 223;
	public static int MERGETYPE_YDDX_BUILDING_HIGH = 224;
	public static int MERGETYPE_YDDX_BUILDING_MID = 225;
	public static int MERGETYPE_YDDX_BUILDING_LOW = 226;
	public static int MERGETYPE_YDLT_CELL = 227;
	public static int MERGETYPE_YDDX_CELL = 228;
	public static int MERGETYPE_YDLT_AREA_GRID = 229;
	public static int MERGETYPE_YDDX_AREA_GRID = 230;
	public static int MERGETYPE_YDLT_AREA_CELL = 231;
	public static int MERGETYPE_YDDX_AREA_CELL = 232;
	public static int MERGETYPE_YDLT_AREA = 233;
	public static int MERGETYPE_YDDX_AREA = 234;
	
	private Map<Integer, IMergeDataDo> mergeDataMap;

	private static MergeDataFactory instance;

	public static MergeDataFactory GetInstance()
	{
		if (instance == null)
		{
			instance = new MergeDataFactory();
		}
		return instance;
	}

	private MergeDataFactory()
	{
		init();
	}

	public boolean init()
	{
		mergeDataMap = new HashMap<Integer, IMergeDataDo>();
		{
			BuildCellMergeDo item = new BuildCellMergeDo();
			item.setDataType(TB_MR_CELLBUILD_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(TB_MR_CELLBUILD_MID);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(TB_MR_CELLBUILD_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeDataDo_2G item = new GridMergeDataDo_2G();
			item.setDataType(MERGETYPE_GRIDSTAT_2G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_2G();
			item.setDataType(MERGETYPE_GRIDSTAT_DT_2G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_2G();
			item.setDataType(MERGETYPE_GRIDSTAT_CQT_2G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeDataDo_3G item = new GridMergeDataDo_3G();
			item.setDataType(MERGETYPE_GRIDSTAT_3G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_3G();
			item.setDataType(MERGETYPE_GRIDSTAT_DT_3G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_3G();
			item.setDataType(MERGETYPE_GRIDSTAT_CQT_3G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellGridMergeDataDo_2G item = new CellGridMergeDataDo_2G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_2G);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_2G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_DT_2G);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_2G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_CQT_2G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellGridMergeDataDo_3G item = new CellGridMergeDataDo_3G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_3G);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_3G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_DT_3G);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_3G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_CQT_3G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellMergeDataDo_2G item = new CellMergeDataDo_2G();
			item.setDataType(MERGETYPE_CELLSTAT_2G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellMergeDataDo_3G item = new CellMergeDataDo_3G();
			item.setDataType(MERGETYPE_CELLSTAT_3G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeDataDo_4G item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_GRIDSTAT_DT_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_GRIDSTAT_CQT_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_GRIDSTAT_ALL_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_DTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CQTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_GRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

		}

		{
			CellMergeDataDo_4G item = new CellMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELLSTAT_4G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellGridMergeDataDo_4G item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELLGRIDSTAT_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELLGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

		}

		{
			UserGridMergeDataDo_4G item = new UserGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_USERGRIDSTAT_4G);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeFreqDataDo_4G item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_GRIDSTAT_DT_4G_FREQ);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_GRIDSTAT_CQT_4G_FREQ);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_FREQ_DTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_FREQ_CQTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellMergeDataDo_Freq item = new CellMergeDataDo_Freq();
			item.setDataType(MERGETYPE_CELLSTAT_FREQ);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			UserActCellMergeDataDo item = new UserActCellMergeDataDo();
			item.setDataType(MERGETYPE_USERACT_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeDataDo_4G item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_GRID_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_DTGRID_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CQTGRID_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_GRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_DTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CQTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellGridMergeDataDo_4G item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CELLGRID_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CELLGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			// dt/cqt cellgrid 20170524
			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELL_GRID_DT);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELL_GRID_CQT);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELL_GRID_DT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_CELL_GRID_CQT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CELL_GRID_DT);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CELL_GRID_CQT);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CELL_GRID_DT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_CELL_GRID_CQT_10);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeFreqDataDo_4G item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_DTGRID_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_CQTGRID_4G);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_DTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_CQTGRID_4G_10);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeFreqByImeiDataDo_4G item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FREQ_GRID_BYIMEI_DT);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FREQ_GRID_BYIMEI_DT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FREQ_GRID_BYIMEI_CQT);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FREQ_GRID_BYIMEI_CQT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT_10);
			mergeDataMap.put(item.getDataType(), item);

			/////
			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_DX_FREQ_GRID_BYIMEI_DT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_DX_FREQ_GRID_BYIMEI_CQT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_DT_10);
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_CQT_10);
			mergeDataMap.put(item.getDataType(), item);

		}
		{
			FreqCellByImeiDataMergeDo item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MERGETYPE_LT_CELL_BYIMEI);
			mergeDataMap.put(item.getDataType(), item);

			item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MERGETYPE_DX_CELL_BYIMEI);
			mergeDataMap.put(item.getDataType(), item);

			item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MERGETYPE_LT_FIGURE_CELL_BYIMEI);
			mergeDataMap.put(item.getDataType(), item);

			item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MERGETYPE_DX_FIGURE_CELL_BYIMEI);
			mergeDataMap.put(item.getDataType(), item);

		}
		{
			OutGridMerge item = new OutGridMerge();
			item.setDataType(MERGETYPE_HIGH_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_MID_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_LOW_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_DX_HIGH_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_DX_MID_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_DX_LOW_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_LT_HIGH_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_LT_MID_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_LT_LOW_OUT_GRID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMerge();
			item.setDataType(MERGETYPE_YDLT_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MERGETYPE_YDLT_OUTGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MERGETYPE_YDLT_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MERGETYPE_YDDX_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MERGETYPE_YDDX_OUTGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MERGETYPE_YDDX_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			OutCellGridMergeDo item = new OutCellGridMergeDo();
			item.setDataType(MERGETYPE_HIGH_OUT_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MERGETYPE_MID_OUT_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MERGETYPE_LOW_OUT_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			InGridMergeDo item = new InGridMergeDo();
			item.setDataType(MERGETYPE_HIGH_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_MID_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_LOW_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_DX_HIGH_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_DX_MID_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_DX_LOW_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_LT_HIGH_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_LT_MID_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_LT_LOW_IN_GRID);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_YDLT_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_YDLT_INGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_YDLT_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_YDDX_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_YDDX_INGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MERGETYPE_YDDX_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			InCellGridMergeDo item = new InCellGridMergeDo();
			item.setDataType(MERGETYPE_HIGH_IN_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MERGETYPE_MID_IN_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MERGETYPE_LOW_IN_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			BuildMergeDo item = new BuildMergeDo();
			item.setDataType(MERGETYPE_HIGH_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_MID_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_LOW_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_DX_HIGH_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_DX_MID_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_DX_LOW_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_LT_HIGH_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_LT_MID_BUILD);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_LT_LOW_BUILD);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_YDLT_BUILDING_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_YDLT_BUILDING_MID);
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_YDLT_BUILDING_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_YDDX_BUILDING_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_YDDX_BUILDING_MID);
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MERGETYPE_YDDX_BUILDING_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellMergeDo item = new CellMergeDo();
			item.setDataType(MERGETYPE_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MERGETYPE_DX_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MERGETYPE_LT_CELL);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new CellMergeDo();
			item.setDataType(MERGETYPE_YDLT_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MERGETYPE_YDDX_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		// scene
		{
			SceneCellGridMergeDo item = new SceneCellGridMergeDo();
			item.setDataType(AREA_CELL_GRID);
			mergeDataMap.put(item.getDataType(), item);
			
		}
		
		{
			SceneCellMergeDo item = new SceneCellMergeDo();
			item.setDataType(AREA_CELL);
			mergeDataMap.put(item.getDataType(), item);
			
			// yzx add LT and DX AREA 2017.9.18
			item = new SceneCellMergeDo();
			item.setDataType(TB_LT_AREA_CELL);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneCellMergeDo();
			item.setDataType(TB_DX_AREA_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MERGETYPE_YDLT_AREA_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MERGETYPE_YDDX_AREA_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			SceneGridMergeDo item = new SceneGridMergeDo();
			item.setDataType(AREA_GRID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneGridMergeDo();
			item.setDataType(TB_LT_AREA_GRID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneGridMergeDo();
			item.setDataType(TB_DX_AREA_GRID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneGridMergeDo();
			item.setDataType(MERGETYPE_YDLT_AREA_GRID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneGridMergeDo();
			item.setDataType(MERGETYPE_YDDX_AREA_GRID);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			SceneMergeDo item = new SceneMergeDo();
			item.setDataType(AREA);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneMergeDo();
			item.setDataType(TB_LT_AREA);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneMergeDo();
			item.setDataType(TB_DX_AREA);
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MERGETYPE_YDLT_AREA);
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MERGETYPE_YDDX_AREA);
			mergeDataMap.put(item.getDataType(), item);
		}
		// mdt merge
		{
			MdtBuild_mergeDo item = new MdtBuild_mergeDo();
			item.setDataType(TB_MDTMR_BUILD_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtBuild_mergeDo();
			item.setDataType(TB_MDTMR_BUILD_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtBuild_mergeDo();
			item.setDataType(TB_LT_MDTMR_BUILD_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtBuild_mergeDo();
			item.setDataType(TB_LT_MDTMR_BUILD_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtBuild_mergeDo();
			item.setDataType(TB_DX_MDTMR_BUILD_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtBuild_mergeDo();
			item.setDataType(TB_DX_MDTMR_BUILD_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtBuildCell_mergeDo item = new MdtBuildCell_mergeDo();
			item.setDataType(TB_MDTMR_BUILDCELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtBuildCell_mergeDo();
			item.setDataType(TB_MDTMR_BUILDCELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtCell_mergeDo item = new MdtCell_mergeDo();
			item.setDataType(TB_MDTMR_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtCell_mergeDo();
			item.setDataType(TB_LT_MDTMR_CELL);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtCell_mergeDo();
			item.setDataType(TB_DX_MDTMR_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtImei_mergeDo item = new MdtImei_mergeDo();
			item.setDataType(TB_MDT_IMEI);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtInCellGrid_mergeDo item = new MdtInCellGrid_mergeDo();
			item.setDataType(TB_MDTMR_INGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtInCellGrid_mergeDo();
			item.setDataType(TB_MDTMR_INGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtInGrid_mergeDo item = new MdtInGrid_mergeDo();
			item.setDataType(TB_MDTMR_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtInGrid_mergeDo();
			item.setDataType(TB_MDTMR_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtInGrid_mergeDo();
			item.setDataType(TB_LT_MDTMR_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtInGrid_mergeDo();
			item.setDataType(TB_LT_MDTMR_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtInGrid_mergeDo();
			item.setDataType(TB_DX_MDTMR_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtInGrid_mergeDo();
			item.setDataType(TB_DX_MDTMR_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtOutCellGrid_mergeDo item = new MdtOutCellGrid_mergeDo();
			item.setDataType(TB_MDTMR_OUTGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtOutCellGrid_mergeDo();
			item.setDataType(TB_MDTMR_OUTGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			MdtOutGrid_mergeDo item = new MdtOutGrid_mergeDo();
			item.setDataType(TB_MDTMR_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtOutGrid_mergeDo();
			item.setDataType(TB_MDTMR_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtOutGrid_mergeDo();
			item.setDataType(TB_LT_MDTMR_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtOutGrid_mergeDo();
			item.setDataType(TB_LT_MDTMR_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtOutGrid_mergeDo();
			item.setDataType(TB_DX_MDTMR_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);

			item = new MdtOutGrid_mergeDo();
			item.setDataType(TB_DX_MDTMR_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		//mdt round grid bigger 9.12
		{
			MdtInCellGrid_mergeBySize item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_YD_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_YD_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_LT_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_LT_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_DX_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_DX_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			MdtInGrid_mergeBySize item = new MdtInGrid_mergeBySize();
			item.setDataType(MDT_ROUND_YD_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInGrid_mergeBySize();
			item.setDataType(MDT_ROUND_YD_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInGrid_mergeBySize();
			item.setDataType(MDT_ROUND_LT_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInGrid_mergeBySize();
			item.setDataType(MDT_ROUND_LT_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInGrid_mergeBySize();
			item.setDataType(MDT_ROUND_DX_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInGrid_mergeBySize();
			item.setDataType(MDT_ROUND_DX_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			MdtInCellGrid_mergeBySize item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_INGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtInCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_INGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			MdtOutCellGrid_mergeBySize item = new MdtOutCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_OUTGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtOutCellGrid_mergeBySize();
			item.setDataType(MDT_ROUND_OUTGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		//mro round grid bigger 9.12
		{
			OutGridMergeBySize item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_YD_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_YD_OUTGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_YD_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_DX_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_DX_OUTGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_DX_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_LT_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_LT_OUTGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMergeBySize();
			item.setDataType(MRO_ROUND_LT_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			InGridMergeBySize item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_YD_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_YD_INGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_YD_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_DX_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_DX_INGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_DX_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_LT_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_LT_INGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeBySize();
			item.setDataType(MRO_ROUND_LT_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			OutCellGridMergeBySize item = new OutCellGridMergeBySize();
			item.setDataType(MRO_ROUND_OUTGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutCellGridMergeBySize();
			item.setDataType(MRO_ROUND_OUTGRID_CELL_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutCellGridMergeBySize();
			item.setDataType(MRO_ROUND_OUTGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			InCellGridMergeBySize item = new InCellGridMergeBySize();
			item.setDataType(MRO_ROUND_INGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InCellGridMergeBySize();
			item.setDataType(MRO_ROUND_INGRID_CELL_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InCellGridMergeBySize();
			item.setDataType(MRO_ROUND_INGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		//yzx add EVENT 2017.9.26
		{
			EventCell_mergeDo item = new EventCell_mergeDo();
			item.setDataType(TB_EVENT_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventInCellGrid_mergeDo item = new EventInCellGrid_mergeDo();
			item.setDataType(TB_EVENT_INGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventInCellGrid_mergeDo();
			item.setDataType(TB_EVENT_INGRID_CELL_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventInCellGrid_mergeDo();
			item.setDataType(TB_EVENT_INGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventInGrid_mergeDo item = new EventInGrid_mergeDo();
			item.setDataType(TB_EVENT_INGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventInGrid_mergeDo();
			item.setDataType(TB_EVENT_INGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventInGrid_mergeDo();
			item.setDataType(TB_EVENT_INGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventOutCellGrid_mergeDo item = new EventOutCellGrid_mergeDo();
			item.setDataType(TB_EVENT_OUTGRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventOutCellGrid_mergeDo();
			item.setDataType(TB_EVENT_OUTGRID_CELL_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventOutCellGrid_mergeDo();
			item.setDataType(TB_EVENT_OUTGRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventOutGrid_mergeDo item = new EventOutGrid_mergeDo();
			item.setDataType(TB_EVENT_OUTGRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventOutGrid_mergeDo();
			item.setDataType(TB_EVENT_OUTGRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventOutGrid_mergeDo();
			item.setDataType(TB_EVENT_OUTGRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventBuildCellGrid_mergeDo item = new EventBuildCellGrid_mergeDo();
			item.setDataType(TB_EVENT_BUILD_GRID_CELL_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventBuildCellGrid_mergeDo();
			item.setDataType(TB_EVENT_BUILD_GRID_CELL_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventBuildCellGrid_mergeDo();
			item.setDataType(TB_EVENT_BUILD_GRID_CELL_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventBuildGrid_mergeDo item = new EventBuildGrid_mergeDo();
			item.setDataType(TB_EVENT_BUILD_GRID_HIGH);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventBuildGrid_mergeDo();
			item.setDataType(TB_EVENT_BUILD_GRID_MID);
			mergeDataMap.put(item.getDataType(), item);
			
			item = new EventBuildGrid_mergeDo();
			item.setDataType(TB_EVENT_BUILD_GRID_LOW);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventArea_mergeDo item = new EventArea_mergeDo();
			item.setDataType(TB_EVENT_AREA);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventAreaGrid_mergeDo item = new EventAreaGrid_mergeDo();
			item.setDataType(TB_EVENT_AREA_GRID);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventAreaCellGrid_mergeDo item = new EventAreaCellGrid_mergeDo();
			item.setDataType(TB_EVENT_AREA_GRID_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventAreaCell_mergeDo item = new EventAreaCell_mergeDo();
			item.setDataType(TB_EVENT_AREA_CELL);
			mergeDataMap.put(item.getDataType(), item);
		}
		
		return true;
	}

	public IMergeDataDo getMergeDataObject(int dataType) throws InstantiationException, IllegalAccessException
	{
		IMergeDataDo mergeDataDo = mergeDataMap.get(dataType);
		IMergeDataDo newItem = mergeDataDo.getClass().newInstance();
		newItem.setDataType(mergeDataDo.getDataType());
		return newItem;
	}

}
