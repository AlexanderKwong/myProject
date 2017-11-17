package mrstat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import jan.com.hadoop.mapred.MultiOutputMng;

public class MroNewTableStat
{
	public static final int DataType_IN_High_Sample = 1;
	public static final int DataType_IN_Mid_Sample = 2;
	public static final int DataType_IN_Low_Sample = 3;
	public static final int DataType_OUT_High_Sample = 4;
	public static final int DataType_OUT_Mid_Sample = 5;
	public static final int DataType_OUT_Low_Sample = 6;

	public static final int DataType_HIGH_OUT_GRID = 9;
	public static final int DataType_MID_OUT_GRID = 10;
	public static final int DataType_LOW_OUT_GRID = 11;

	public static final int DataType_HIGH_OUT_GRID_CELL = 12;
	public static final int DataType_MID_OUT_GRID_CELL = 13;
	public static final int DataType_LOW_OUT_GRID_CELL = 14;

	public static final int DataType_HIGH_IN_GRID = 15;
	public static final int DataType_MID_IN_GRID = 16;
	public static final int DataType_LOW_IN_GRID = 17;

	public static final int DataType_HIGH_IN_GRID_CELL = 18;
	public static final int DataType_MID_IN_GRID_CELL = 19;
	public static final int DataType_LOW_IN_GRID_CELL = 20;

	public static final int DataType_HIGH_BUILD = 21;
	public static final int DataType_MID_BUILD = 22;
	public static final int DataType_LOW_BUILD = 23;

	public static final int DataType_CELL = 24;

	public static final int DataType_DX_HIGH_OUT_GRID = 25;
	public static final int DataType_DX_MID_OUT_GRID = 26;
	public static final int DataType_DX_LOW_OUT_GRID = 27;

	public static final int DataType_DX_HIGH_IN_GRID = 28;
	public static final int DataType_DX_MID_IN_GRID = 29;
	public static final int DataType_DX_LOW_IN_GRID = 30;

	public static final int DataType_DX_HIGH_BUILD = 31;
	public static final int DataType_DX_MID_BUILD = 32;
	public static final int DataType_DX_LOW_BUILD = 33;

	public static final int DataType_DX_CELL = 34;

	public static final int DataType_LT_HIGH_OUT_GRID = 35;
	public static final int DataType_LT_MID_OUT_GRID = 36;
	public static final int DataType_LT_LOW_OUT_GRID = 37;

	public static final int DataType_LT_HIGH_IN_GRID = 38;
	public static final int DataType_LT_MID_IN_GRID = 39;
	public static final int DataType_LT_LOW_IN_GRID = 40;

	public static final int DataType_LT_HIGH_BUILD = 41;
	public static final int DataType_LT_MID_BUILD = 42;
	public static final int DataType_LT_LOW_BUILD = 43;

	public static final int DataType_LT_CELL = 44;

	public static final int DataType_AREA_CELL = 45;
	public static final int DataType_AREA_CELL_GRID = 46;
	public static final int DataType_AREA_GRID = 47;
	public static final int DataType_AREA = 48;
	// yzx add 2017.9.14
	public static final int DataType_AREA_Sample = 49;

	public static final int DataType_Vap_tbMr = 50;

	public static final int DataType_HIGH_BUILDCELL = 51;
	public static final int DataType_MID_BUILDCELL = 52;
	public static final int DataType_LOW_BUILDCELL = 53;

	// yzx add 2017.9.14
	public static final int DataType_LT_AREA_CELL = 54;
	public static final int DataType_LT_AREA_GRID = 56;
	public static final int DataType_LT_AREA = 57;

	public static final int DataType_DX_AREA_CELL = 58;
	public static final int DataType_DX_AREA_GRID = 60;
	public static final int DataType_DX_AREA = 61;

// 20170920 add 全网通 相关统计
	public static final int DataType_YDLT_HIGH_OUT_GRID = 62;
	public static final int DataType_YDLT_MID_OUT_GRID = 63;
	public static final int DataType_YDLT_LOW_OUT_GRID = 64;

	public static final int DataType_YDLT_HIGH_IN_GRID = 65;
	public static final int DataType_YDLT_MID_IN_GRID = 66;
	public static final int DataType_YDLT_LOW_IN_GRID = 67;

	public static final int DataType_YDLT_HIGH_BUILD = 68;
	public static final int DataType_YDLT_MID_BUILD = 69;
	public static final int DataType_YDLT_LOW_BUILD = 70;

	public static final int DataType_YDLT_CELL = 71;

	public static final int DataType_YDLT_AREA_CELL = 72;
//	public static final int DataType_YDLT_AREA_CELL_GRID = 73;
	public static final int DataType_YDLT_AREA_GRID = 73;
	public static final int DataType_YDLT_AREA = 74;

	public static final int DataType_YDDX_HIGH_OUT_GRID = 75;
	public static final int DataType_YDDX_MID_OUT_GRID = 76;
	public static final int DataType_YDDX_LOW_OUT_GRID = 77;

	public static final int DataType_YDDX_HIGH_IN_GRID = 78;
	public static final int DataType_YDDX_MID_IN_GRID = 79;
	public static final int DataType_YDDX_LOW_IN_GRID = 80;

	public static final int DataType_YDDX_HIGH_BUILD = 81;
	public static final int DataType_YDDX_MID_BUILD = 82;
	public static final int DataType_YDDX_LOW_BUILD = 83;

	public static final int DataType_YDDX_CELL = 84;

	public static final int DataType_YDDX_AREA_CELL = 85;
	public static final int DataType_YDDX_AREA_GRID = 86;
	public static final int DataType_YDDX_AREA = 87;	
// OUTPUT_PATH
	public static final String MRO_INSAMPLE_HIGH = "/tb_mr_insample_high_dd_";
	public static final String MRO_INSAMPLE_MID = "/tb_mr_insample_mid_dd_";
	public static final String MRO_INSAMPLE_LOW = "/tb_mr_insample_low_dd_";
	public static final String MRO_OUTSAMPLE_HIGH = "/tb_mr_outsample_high_dd_";
	public static final String MRO_OUTSAMPLE_MID = "/tb_mr_outsample_mid_dd_";
	public static final String MRO_OUTSAMPLE_LOW = "/tb_mr_outsample_low_dd_";
	public static final String MRO_YD_OUTGRID_HIGH = "/tb_mr_outgrid_high_yd_dd_";
	public static final String MRO_YD_OUTGRID_MID = "/tb_mr_outgrid_mid_yd_dd_";
	public static final String MRO_YD_OUTGRID_LOW = "/tb_mr_outgrid_low_yd_dd_";
	public static final String MRO_OUTGRID_CELL_HIGH = "/tb_mr_outgrid_cell_high_dd_";
	public static final String MRO_OUTGRID_CELL_MID = "/tb_mr_outgrid_cell_mid_dd_";
	public static final String MRO_OUTGRID_CELL_LOW = "/tb_mr_outgrid_cell_low_dd_";
	public static final String MRO_YD_INGRID_HIGH = "/tb_mr_ingrid_high_yd_dd_";
	public static final String MRO_YD_INGRID_MID = "/tb_mr_ingrid_mid_yd_dd_";
	public static final String MRO_YD_INGRID_LOW = "/tb_mr_ingrid_low_yd_dd_";
	public static final String MRO_INGRID_CELL_HIGH = "/tb_mr_ingrid_cell_high_dd_";
	public static final String MRO_INGRID_CELL_MID = "/tb_mr_ingrid_cell_mid_dd_";
	public static final String MRO_INGRID_CELL_LOW = "/tb_mr_ingrid_cell_low_dd_";
	public static final String MRO_YD_BUILD_HIGH = "/tb_mr_building_high_yd_dd_";
	public static final String MRO_YD_BUILD_MID = "/tb_mr_building_mid_yd_dd_";
	public static final String MRO_YD_BUILD_LOW = "/tb_mr_building_low_yd_dd_";
	public static final String MRO_YD_CELL = "/tb_mr_cell_yd_dd_";
	public static final String MRO_DX_OUTGRID_HIGH = "/tb_mr_outgrid_high_dx_dd_";
	public static final String MRO_DX_OUTGRID_MID = "/tb_mr_outgrid_mid_dx_dd_";
	public static final String MRO_DX_OUTGRID_LOW = "/tb_mr_outgrid_low_dx_dd_";
	public static final String MRO_DX_INGRID_HIGH = "/tb_mr_ingrid_high_dx_dd_";
	public static final String MRO_DX_INGRID_MID = "/tb_mr_ingrid_mid_dx_dd_";
	public static final String MRO_DX_INGRID_LOW = "/tb_mr_ingrid_low_dx_dd_";
	public static final String MRO_DX_BUILD_HIGH = "/tb_mr_building_high_dx_dd_";
	public static final String MRO_DX_BUILD_MID = "/tb_mr_building_mid_dx_dd_";
	public static final String MRO_DX_BUILD_LOW = "/tb_mr_building_low_dx_dd_";
	public static final String MRO_DX_CELL = "/tb_mr_cell_dx_dd_";
	public static final String MRO_LT_OUTGRID_HIGH = "/tb_mr_outgrid_high_lt_dd_";
	public static final String MRO_LT_OUTGRID_MID = "/tb_mr_outgrid_mid_lt_dd_";
	public static final String MRO_LT_OUTGRID_LOW = "/tb_mr_outgrid_low_lt_dd_";
	public static final String MRO_LT_INGRID_HIGH = "/tb_mr_ingrid_high_lt_dd_";
	public static final String MRO_LT_INGRID_MID = "/tb_mr_ingrid_mid_lt_dd_";
	public static final String MRO_LT_INGRID_LOW = "/tb_mr_ingrid_low_lt_dd_";
	public static final String MRO_LT_BUILD_HIGH = "/tb_mr_building_high_lt_dd_";
	public static final String MRO_LT_BUILD_MID = "/tb_mr_building_mid_lt_dd_";
	public static final String MRO_LT_BUILD_LOW = "/tb_mr_building_low_lt_dd_";
	public static final String MRO_LT_CELL = "/tb_mr_cell_lt_dd_";
	public static final String MRO_YD_AREA_CELL = "/tb_mr_area_cell_yd_dd_";
	public static final String MRO_YD_AREA_GRID_CELL = "/tb_mr_area_outgrid_cell_dd_";
	public static final String MRO_YD_AREA_GRID = "/tb_mr_area_outgrid_yd_dd_";
	public static final String MRO_YD_AREA = "/tb_mr_area_yd_dd_";
	// yzx add 2017.9.14
	public static final String MRO_AREA_Sample = "/tb_mr_area_sample_dd_";

	public static final String MRO_VAP = "/tb_mr_vap_";
	public static final String MRO_BUILD_CELL_HIGH = "/tb_mr_building_cell_high_dd_";
	public static final String MRO_BUILD_CELL_MID = "/tb_mr_building_cell_mid_dd_";
	public static final String MRO_BUILD_CELL_LOW = "/tb_mr_building_cell_low_dd_";

	// yzx add 2017.9.14
	public static final String MRO_LT_AREA_CELL = "/tb_mr_area_cell_lt_dd_";
	public static final String MRO_LT_AREA_GRID = "/tb_mr_area_outgrid_lt_dd_";
	public static final String MRO_LT_AREA = "/tb_mr_area_lt_dd_";

	public static final String MRO_DX_AREA_CELL = "/tb_mr_area_cell_dx_dd_";
	public static final String MRO_DX_AREA_GRID = "/tb_mr_area_outgrid_dx_dd_";
	public static final String MRO_DX_AREA = "/tb_mr_area_dx_dd_";

	// yzx add 2017.9.26
	public static final String EVENT_CELL = "/tb_evt_cell_dd_";

	public static final String EVENT_INGRID_CELL_HIGH = "/tb_evt_ingrid_cell_high_dd_";
	public static final String EVENT_INGRID_CELL_MID = "/tb_evt_ingrid_cell_mid_dd_";
	public static final String EVENT_INGRID_CELL_LOW = "/tb_evt_ingrid_cell_low_dd_";

	public static final String EVENT_INGRID_HIGH = "/tb_evt_ingrid_high_dd_";
	public static final String EVENT_INGRID_MID = "/tb_evt_ingrid_mid_dd_";
	public static final String EVENT_INGRID_LOW = "/tb_evt_ingrid_low_dd_";

	public static final String EVENT_OUTGRID_CELL_HIGH = "/tb_evt_outgrid_cell_high_dd_";
	public static final String EVENT_OUTGRID_CELL_MID = "/tb_evt_outgrid_cell_mid_dd_";
	public static final String EVENT_OUTGRID_CELL_LOW = "/tb_evt_outgrid_cell_low_dd_";

	public static final String EVENT_OUTGRID_HIGH = "/tb_evt_outgrid_high_dd_";
	public static final String EVENT_OUTGRID_MID = "/tb_evt_outgrid_mid_dd_";
	public static final String EVENT_OUTGRID_LOW = "/tb_evt_outgrid_low_dd_";

	public static final String EVENT_BUILD_GRID_CELL_HIGH = "/tb_evt_building_cell_high_dd_";
	public static final String EVENT_BUILD_GRID_CELL_MID = "/tb_evt_building_cell_mid_dd_";
	public static final String EVENT_BUILD_GRID_CELL_LOW = "/tb_evt_building_cell_low_dd_";

	public static final String EVENT_BUILD_GRID_HIGH = "/tb_evt_building_high_dd_";
	public static final String EVENT_BUILD_GRID_MID = "/tb_evt_building_mid_dd_";
	public static final String EVENT_BUILD_GRID_LOW = "/tb_evt_building_low_dd_";

	public static final String EVENT_AREA = "/tb_evt_area_dd_";
	public static final String EVENT_AREA_GRID = "/tb_evt_area_outgrid_dd_";
	public static final String EVENT_AREA_GRID_CELL = "/tb_evt_area_outgrid_cell_dd_";
	public static final String EVENT_AREA_CELL = "/tb_evt_area_cell_dd_";

// 20170921 add fullnet output
	public static final String MRO_YDLT_OUTGRID_HIGH = "/tb_mr_outgrid_fullnet_high_ydlt_dd_";
	public static final String MRO_YDLT_OUTGRID_MID = "/tb_mr_outgrid_fullnet_mid_ydlt_dd_";
	public static final String MRO_YDLT_OUTGRID_LOW = "/tb_mr_outgrid_fullnet_low_ydlt_dd_";
	public static final String MRO_YDDX_OUTGRID_HIGH = "/tb_mr_outgrid_fullnet_high_yddx_dd_";
	public static final String MRO_YDDX_OUTGRID_MID = "/tb_mr_outgrid_fullnet_mid_yddx_dd_";
	public static final String MRO_YDDX_OUTGRID_LOW = "/tb_mr_outgrid_fullnet_low_yddx_dd_";
	public static final String MRO_YDLT_INGRID_HIGH = "/tb_mr_ingrid_fullnet_high_ydlt_dd_";
	public static final String MRO_YDLT_INGRID_MID = "/tb_mr_ingrid_fullnet_mid_ydlt_dd_";
	public static final String MRO_YDLT_INGRID_LOW = "/tb_mr_ingrid_fullnet_low_ydlt_dd_";
	public static final String MRO_YDDX_INGRID_HIGH = "/tb_mr_ingrid_fullnet_high_yddx_dd_";
	public static final String MRO_YDDX_INGRID_MID = "/tb_mr_ingrid_fullnet_mid_yddx_dd_";
	public static final String MRO_YDDX_INGRID_LOW = "/tb_mr_ingrid_fullnet_low_yddx_dd_";
	public static final String MRO_YDLT_BUILDING_HIGH = "/tb_mr_building_fullnet_high_ydlt_dd_";
	public static final String MRO_YDLT_BUILDING_MID = "/tb_mr_building_fullnet_mid_ydlt_dd_";
	public static final String MRO_YDLT_BUILDING_LOW = "/tb_mr_building_fullnet_low_ydlt_dd_";
	public static final String MRO_YDDX_BUILDING_HIGH = "/tb_mr_building_fullnet_high_yddx_dd_";
	public static final String MRO_YDDX_BUILDING_MID = "/tb_mr_building_fullnet_mid_yddx_dd_";
	public static final String MRO_YDDX_BUILDING_LOW = "/tb_mr_building_fullnet_low_yddx_dd_";
	public static final String MRO_YDLT_CELL = "/tb_mr_cell_fullnet_ydlt_dd_";
	public static final String MRO_YDDX_CELL = "/tb_mr_cell_fullnet_yddx_dd_";
	public static final String MRO_YDLT_AREA_GRID = "/tb_mr_area_grid_fullnet_ydlt_dd_";
	public static final String MRO_YDDX_AREA_GRID = "/tb_mr_area_grid_fullnet_yddx_dd_";
	public static final String MRO_YDLT_AREA_CELL = "/tb_mr_area_cell_fullnet_ydlt_dd_";
	public static final String MRO_YDDX_AREA_CELL = "/tb_mr_area_cell_fullnet_yddx_dd_";
	public static final String MRO_YDLT_AREA = "/tb_mr_area_fullnet_ydlt_dd_";
	public static final String MRO_YDDX_AREA = "/tb_mr_area_fullnet_yddx_dd_";
	public static void registerOutFileName(Job job)
	{
		MultipleOutputs.addNamedOutput(job, "inhighsample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "inmidsample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "inlowsample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "outhighsample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "outmidsample", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "outlowsample", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "highOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "midOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lowOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "highOutGridCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "midOutGridCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lowOutGridCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "highInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "midInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lowInGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "highInGridCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "midInGridCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lowInGridCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "highBuild", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "midBuild", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lowBuild", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "cell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "dxHighOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxMidOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxLowOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "dxHighInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxMidInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxLowInGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "dxHighBuild", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxMidBuild", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxLowBuild", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "dxCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "ltHighOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltMidOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltLowOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "ltHighInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltMidInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltLowInGrid", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "ltHighBuild", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltMidBuild", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltLowBuild", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "ltCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "sceneCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sceneCellGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sceneGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "scene", TextOutputFormat.class, NullWritable.class, Text.class);
		// yzx add 2017.9.14
		MultipleOutputs.addNamedOutput(job, "sceneSample", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "vaptbmr", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "highbuildcell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "midbuildcell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lowbuildcell", TextOutputFormat.class, NullWritable.class, Text.class);

		// yzx add 2017.9.14
		MultipleOutputs.addNamedOutput(job, "ltSceneCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltSceneGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ltScene", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "dxSceneCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxSceneGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "dxScene", TextOutputFormat.class, NullWritable.class, Text.class);

// 20170921 add
		MultipleOutputs.addNamedOutput(job, "ydltHighOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltMidOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltLowOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxHighOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxMidOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxLowOutGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltHighInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltMidInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltLowInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxHighInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxMidInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxLowInGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltHighBuilding", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltMidBuilding", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltLowBuilding", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxHighBuilding", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxMidBuilding", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxLowBuilding", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltAreaGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxAreaGrid", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltAreaGridCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxAreaGridCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltAreaCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxAreaCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ydltArea", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "yddxArea", TextOutputFormat.class, NullWritable.class, Text.class);
		}

	public static void getOutPutPackage(MultiOutputMng<NullWritable, Text> mosMng, TypeInfoMng typeInfoMng, String outpath_table, String dateStr)
	{

		String filePath = outpath_table + MRO_INSAMPLE_HIGH + dateStr;
		TypeInfo typeInfo = new TypeInfo(DataType_IN_High_Sample, "inhighsample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("inhighsample", filePath);

		filePath = outpath_table + MRO_INSAMPLE_MID + dateStr;
		typeInfo = new TypeInfo(DataType_IN_Mid_Sample, "inmidsample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("inmidsample", filePath);

		filePath = outpath_table + MRO_INSAMPLE_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_IN_Low_Sample, "inlowsample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("inlowsample", filePath);

		filePath = outpath_table + MRO_OUTSAMPLE_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_OUT_High_Sample, "outhighsample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("outhighsample", filePath);

		filePath = outpath_table + MRO_OUTSAMPLE_MID + dateStr;
		typeInfo = new TypeInfo(DataType_OUT_Mid_Sample, "outmidsample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("outmidsample", filePath);

		filePath = outpath_table + MRO_OUTSAMPLE_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_OUT_Low_Sample, "outlowsample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("outlowsample", filePath);

		// 新添加统计表
		filePath = outpath_table + MRO_YD_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_HIGH_OUT_GRID, "highOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("highOutGrid", filePath);

		filePath = outpath_table + MRO_YD_OUTGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_MID_OUT_GRID, "midOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("midOutGrid", filePath);

		filePath = outpath_table + MRO_YD_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LOW_OUT_GRID, "lowOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("lowOutGrid", filePath);

		filePath = outpath_table + MRO_OUTGRID_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_HIGH_OUT_GRID_CELL, "highOutGridCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("highOutGridCell", filePath);

		filePath = outpath_table + MRO_OUTGRID_CELL_MID + dateStr;
		typeInfo = new TypeInfo(DataType_MID_OUT_GRID_CELL, "midOutGridCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("midOutGridCell", filePath);

		filePath = outpath_table + MRO_OUTGRID_CELL_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LOW_OUT_GRID_CELL, "lowOutGridCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("lowOutGridCell", filePath);

		filePath = outpath_table + MRO_YD_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_HIGH_IN_GRID, "highInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("highInGrid", filePath);

		filePath = outpath_table + MRO_YD_INGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_MID_IN_GRID, "midInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("midInGrid", filePath);

		filePath = outpath_table + MRO_YD_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LOW_IN_GRID, "lowInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("lowInGrid", filePath);

		filePath = outpath_table + MRO_INGRID_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_HIGH_IN_GRID_CELL, "highInGridCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("highInGridCell", filePath);

		filePath = outpath_table + MRO_INGRID_CELL_MID + dateStr;
		typeInfo = new TypeInfo(DataType_MID_IN_GRID_CELL, "midInGridCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("midInGridCell", filePath);

		filePath = outpath_table + MRO_INGRID_CELL_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LOW_IN_GRID_CELL, "lowInGridCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("lowInGridCell", filePath);

		filePath = outpath_table + MRO_YD_BUILD_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_HIGH_BUILD, "highBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("highBuild", filePath);

		filePath = outpath_table + MRO_YD_BUILD_MID + dateStr;
		typeInfo = new TypeInfo(DataType_MID_BUILD, "midBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("midBuild", filePath);

		filePath = outpath_table + MRO_YD_BUILD_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LOW_BUILD, "lowBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("lowBuild", filePath);

		filePath = outpath_table + MRO_YD_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_CELL, "cell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("cell", filePath);

		filePath = outpath_table + MRO_DX_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_DX_HIGH_OUT_GRID, "dxHighOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxHighOutGrid", filePath);

		filePath = outpath_table + MRO_DX_OUTGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_DX_MID_OUT_GRID, "dxMidOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxMidOutGrid", filePath);

		filePath = outpath_table + MRO_DX_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_DX_LOW_OUT_GRID, "dxLowOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxLowOutGrid", filePath);

		filePath = outpath_table + MRO_DX_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_DX_HIGH_IN_GRID, "dxHighInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxHighInGrid", filePath);

		filePath = outpath_table + MRO_DX_INGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_DX_MID_IN_GRID, "dxMidInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxMidInGrid", filePath);

		filePath = outpath_table + MRO_DX_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_DX_LOW_IN_GRID, "dxLowInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxLowInGrid", filePath);

		filePath = outpath_table + MRO_DX_BUILD_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_DX_HIGH_BUILD, "dxHighBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxHighBuild", filePath);

		filePath = outpath_table + MRO_DX_BUILD_MID + dateStr;
		typeInfo = new TypeInfo(DataType_DX_MID_BUILD, "dxMidBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxMidBuild", filePath);

		filePath = outpath_table + MRO_DX_BUILD_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_DX_LOW_BUILD, "dxLowBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxLowBuild", filePath);

		filePath = outpath_table + MRO_DX_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_DX_CELL, "dxCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxCell", filePath);

		filePath = outpath_table + MRO_LT_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_LT_HIGH_OUT_GRID, "ltHighOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltHighOutGrid", filePath);

		filePath = outpath_table + MRO_LT_OUTGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_LT_MID_OUT_GRID, "ltMidOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltMidOutGrid", filePath);

		filePath = outpath_table + MRO_LT_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LT_LOW_OUT_GRID, "ltLowOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltLowOutGrid", filePath);

		filePath = outpath_table + MRO_LT_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_LT_HIGH_IN_GRID, "ltHighInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltHighInGrid", filePath);

		filePath = outpath_table + MRO_LT_INGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_LT_MID_IN_GRID, "ltMidInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltMidInGrid", filePath);

		filePath = outpath_table + MRO_LT_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LT_LOW_IN_GRID, "ltLowInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltLowInGrid", filePath);

		filePath = outpath_table + MRO_LT_BUILD_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_LT_HIGH_BUILD, "ltHighBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltHighBuild", filePath);

		filePath = outpath_table + MRO_LT_BUILD_MID + dateStr;
		typeInfo = new TypeInfo(DataType_LT_MID_BUILD, "ltMidBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltMidBuild", filePath);

		filePath = outpath_table + MRO_LT_BUILD_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LT_LOW_BUILD, "ltLowBuild");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltLowBuild", filePath);

		filePath = outpath_table + MRO_LT_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_LT_CELL, "ltCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltCell", filePath);

		filePath = outpath_table + MRO_YD_AREA_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_AREA_CELL, "sceneCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("sceneCell", filePath);

		filePath = outpath_table + MRO_YD_AREA_GRID_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_AREA_CELL_GRID, "sceneCellGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("sceneCellGrid", filePath);

		filePath = outpath_table + MRO_YD_AREA_GRID + dateStr;
		typeInfo = new TypeInfo(DataType_AREA_GRID, "sceneGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("sceneGrid", filePath);

		filePath = outpath_table + MRO_YD_AREA + dateStr;
		typeInfo = new TypeInfo(DataType_AREA, "scene");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("scene", filePath);

		// yzx add 2017.9.14
		filePath = outpath_table + MRO_AREA_Sample + dateStr;
		typeInfo = new TypeInfo(DataType_AREA_Sample, "sceneSample");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("sceneSample", filePath);

		filePath = outpath_table + MRO_VAP + dateStr;
		typeInfo = new TypeInfo(DataType_Vap_tbMr, "vaptbmr");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("vaptbmr", filePath);

		filePath = outpath_table + MRO_BUILD_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_HIGH_BUILDCELL, "highbuildcell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("highbuildcell", filePath);

		filePath = outpath_table + MRO_BUILD_CELL_MID + dateStr;
		typeInfo = new TypeInfo(DataType_MID_BUILDCELL, "midbuildcell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("midbuildcell", filePath);

		filePath = outpath_table + MRO_BUILD_CELL_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_LOW_BUILDCELL, "lowbuildcell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("lowbuildcell", filePath);

		// yzx add 2017.9.14
		filePath = outpath_table + MRO_LT_AREA_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_LT_AREA_CELL, "ltSceneCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltSceneCell", filePath);

		filePath = outpath_table + MRO_LT_AREA_GRID + dateStr;
		typeInfo = new TypeInfo(DataType_LT_AREA_GRID, "ltSceneGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltSceneGrid", filePath);

		filePath = outpath_table + MRO_LT_AREA + dateStr;
		typeInfo = new TypeInfo(DataType_LT_AREA, "ltScene");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ltScene", filePath);

		filePath = outpath_table + MRO_DX_AREA_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_DX_AREA_CELL, "dxSceneCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxSceneCell", filePath);

		filePath = outpath_table + MRO_DX_AREA_GRID + dateStr;
		typeInfo = new TypeInfo(DataType_DX_AREA_GRID, "dxSceneGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxSceneGrid", filePath);

		filePath = outpath_table + MRO_DX_AREA + dateStr;
		typeInfo = new TypeInfo(DataType_DX_AREA, "dxScene");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("dxScene", filePath);
// 20170921 add
		filePath = outpath_table + MRO_YDLT_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_HIGH_OUT_GRID, "ydltHighOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltHighOutGrid", filePath);

		filePath = outpath_table + MRO_YDLT_OUTGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_MID_OUT_GRID, "ydltMidOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltMidOutGrid", filePath);

		filePath = outpath_table + MRO_YDLT_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_LOW_OUT_GRID, "ydltLowOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltLowOutGrid", filePath);

		filePath = outpath_table + MRO_YDDX_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_HIGH_OUT_GRID, "yddxHighOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxHighOutGrid", filePath);

		filePath = outpath_table + MRO_YDDX_OUTGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_MID_OUT_GRID, "yddxMidOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxMidOutGrid", filePath);

		filePath = outpath_table + MRO_YDDX_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_LOW_OUT_GRID, "yddxLowOutGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxLowOutGrid", filePath);

		filePath = outpath_table + MRO_YDLT_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_HIGH_IN_GRID, "ydltHighInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltHighInGrid", filePath);

		filePath = outpath_table + MRO_YDLT_INGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_MID_IN_GRID, "ydltMidInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltMidInGrid", filePath);

		filePath = outpath_table + MRO_YDLT_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_LOW_IN_GRID, "ydltLowInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltLowInGrid", filePath);

		filePath = outpath_table + MRO_YDDX_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_HIGH_IN_GRID, "yddxHighInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxHighInGrid", filePath);

		filePath = outpath_table + MRO_YDDX_INGRID_MID + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_MID_IN_GRID, "yddxMidInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxMidInGrid", filePath);

		filePath = outpath_table + MRO_YDDX_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_LOW_IN_GRID, "yddxLowInGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxLowInGrid", filePath);

		filePath = outpath_table + MRO_YDLT_BUILDING_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_HIGH_BUILD, "ydltHighBuilding");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltHighBuilding", filePath);

		filePath = outpath_table + MRO_YDLT_BUILDING_MID + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_MID_BUILD, "ydltMidBuilding");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltMidBuilding", filePath);

		filePath = outpath_table + MRO_YDLT_BUILDING_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_LOW_BUILD, "ydltLowBuilding");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltLowBuilding", filePath);

		filePath = outpath_table + MRO_YDDX_BUILDING_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_HIGH_BUILD, "yddxHighBuilding");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxHighBuilding", filePath);

		filePath = outpath_table + MRO_YDDX_BUILDING_MID + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_MID_BUILD, "yddxMidBuilding");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxMidBuilding", filePath);

		filePath = outpath_table + MRO_YDDX_BUILDING_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_LOW_BUILD, "yddxLowBuilding");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxLowBuilding", filePath);

		filePath = outpath_table + MRO_YDLT_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_CELL, "ydltCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltCell", filePath);

		filePath = outpath_table + MRO_YDDX_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_CELL, "yddxCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxCell", filePath);

		filePath = outpath_table + MRO_YDLT_AREA_GRID + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_AREA_GRID, "ydltAreaGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltAreaGrid", filePath);

		filePath = outpath_table + MRO_YDDX_AREA_GRID + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_AREA_GRID, "yddxAreaGrid");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxAreaGrid", filePath);

		filePath = outpath_table + MRO_YDLT_AREA_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_AREA_CELL, "ydltAreaCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltAreaCell", filePath);

		filePath = outpath_table + MRO_YDDX_AREA_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_AREA_CELL, "yddxAreaCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxAreaCell", filePath);

		filePath = outpath_table + MRO_YDLT_AREA + dateStr;
		typeInfo = new TypeInfo(DataType_YDLT_AREA, "ydltArea");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("ydltArea", filePath);

		filePath = outpath_table + MRO_YDDX_AREA + dateStr;
		typeInfo = new TypeInfo(DataType_YDDX_AREA, "yddxArea");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("yddxArea", filePath);	}
}
