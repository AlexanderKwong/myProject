package mdtstat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import jan.com.hadoop.mapred.MultiOutputMng;
import mrstat.TypeInfo;
import mrstat.TypeInfoMng;

public class MdtNewTableStat
{
	public static final int DataType_TB_MDTMR_BUILD_HIGH = 1;
	public static final int DataType_TB_MDTMR_BUILD_LOW = 2;
	public static final int DataType_TB_MDTMR_OUTGRID_HIGH = 3;
	public static final int DataType_TB_MDTMR_OUTGRID_LOW = 4;
	public static final int DataType_TB_MDTMR_INGRID_HIGH = 5;
	public static final int DataType_TB_MDTMR_INGRID_LOW = 6;
	public static final int DataType_TB_MDTMR_INGRID_CELL_HIGH = 7;
	public static final int DataType_TB_MDTMR_INGRID_CELL_LOW = 8;
	public static final int DataType_TB_MDTMR_OUTGRID_CELL_HIGH = 9;
	public static final int DataType_TB_MDTMR_OUTGRID_CELL_LOW = 10;
	public static final int DataType_TB_MDTMR_CELL = 11;
	public static final int DataType_TB_MDT_IMEI = 12;

	public static final int DataType_TB_LT_MDTMR_BUILD_HIGH = 13;
	public static final int DataType_TB_LT_MDTMR_BUILD_LOW = 14;
	public static final int DataType_TB_LT_MDTMR_OUTGRID_HIGH = 15;
	public static final int DataType_TB_LT_MDTMR_OUTGRID_LOW = 16;
	public static final int DataType_TB_LT_MDTMR_INGRID_HIGH = 17;
	public static final int DataType_TB_LT_MDTMR_INGRID_LOW = 18;
	public static final int DataType_TB_LT_MDTMR_CELL = 19;

	public static final int DataType_TB_DX_MDTMR_BUILD_HIGH = 20;
	public static final int DataType_TB_DX_MDTMR_BUILD_LOW = 21;
	public static final int DataType_TB_DX_MDTMR_OUTGRID_HIGH = 22;
	public static final int DataType_TB_DX_MDTMR_OUTGRID_LOW = 23;
	public static final int DataType_TB_DX_MDTMR_INGRID_HIGH = 24;
	public static final int DataType_TB_DX_MDTMR_INGRID_LOW = 25;
	public static final int DataType_TB_DX_MDTMR_CELL = 26;

	public static final int DataType_TB_MDTMR_BUILDCELL_HIGH = 27;
	public static final int DataType_TB_MDTMR_BUILDCELL_LOW = 28;

	// OUTPUT_NAME

	public static final String MDT_YD_BUILD_HIGH = "/tb_mdtmr_building_high_yd_dd_";
	public static final String MDT_YD_BUILD_LOW = "/tb_mdtmr_building_low_yd_dd_";
	public static final String MDT_YD_OUTGRID_HIGH = "/tb_mdtmr_outgrid_high_yd_dd_";
	public static final String MDT_YD_OUTGRID_LOW = "/tb_mdtmr_outgrid_low_yd_dd_";
	public static final String MDT_YD_INGRID_HIGH = "/tb_mdtmr_ingrid_high_yd_dd_";
	public static final String MDT_YD_INGRID_LOW = "/tb_mdtmr_ingrid_low_yd_dd_";
	public static final String MDT_INGRID_CELL_HIGH = "/tb_mdtmr_ingrid_cell_high_dd_";
	public static final String MDT_INGRID_CELL_LOW = "/tb_mdtmr_ingrid_cell_low_dd_";
	public static final String MDT_OUTGRID_CELL_HIGH = "/tb_mdtmr_outgrid_cell_high_dd_";
	public static final String MDT_OUTGRID_CELL_LOW = "/tb_mdtmr_outgrid_cell_low_dd_";
	public static final String MDT_YD_CELL_HIGH = "/tb_mdtmr_cell_yd_dd_";
	public static final String MDT_IMEI = "/tb_mdt_imei_dd_";

	// --LT
	public static final String MDT_LT_BUILD_HIGH = "/tb_mdtmr_building_high_lt_dd_";
	public static final String MDT_LT_BUILD_LOW = "/tb_mdtmr_building_low_lt_dd_";
	public static final String MDT_LT_OUTGRID_HIGH = "/tb_mdtmr_outgrid_high_lt_dd_";
	public static final String MDT_LT_OUTGRID_LOW = "/tb_mdtmr_outgrid_low_lt_dd_";
	public static final String MDT_LT_INGRID_HIGH = "/tb_mdtmr_ingrid_high_lt_dd_";
	public static final String MDT_LT_INGRID_LOW = "/tb_mdtmr_ingrid_low_lt_dd_";
	public static final String MDT_LT_CELL = "/tb_mdtmr_cell_lt_dd_";

	// --DX
	public static final String MDT_DX_BUILD_HIGH = "/tb_mdtmr_building_high_dx_dd_";
	public static final String MDT_DX_BUILD_LOW = "/tb_mdtmr_building_high_dx_dd_";
	public static final String MDT_DX_OUTGRID_HIGH = "/tb_mdtmr_outgrid_high_dx_dd_";
	public static final String MDT_DX_OUTGRID_LOW = "/tb_mdtmr_outgrid_low_dx_dd_";
	public static final String MDT_DX_INGRID_HIGH = "/tb_mdtmr_ingrid_high_dx_dd_";
	public static final String MDT_DX_INGRID_LOW = "/tb_mdtmr_ingrid_low_dx_dd_";
	public static final String MDT_DX_CELL = "/tb_mdtmr_cell_dx_dd_";

	public static final String MDT_BUILD_CELL_HIGH = "/tb_mdtmr_building_cell_high_dd_";
	public static final String MDT_BUILD_CELL_LOW = "/tb_mdtmr_building_cell_low_dd_";

	public static void registerOutFileName(Job job)
	{
		MultipleOutputs.addNamedOutput(job, "mdtBuildHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtBuildLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtOutGridHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtOutGridLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtInGridHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtInGridLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtInGridCellHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtInGridCellLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtOutGridCellHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtOutGridCellLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtMrCell", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtImei", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "mdtLTBuildHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtLTBuildLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtLTOutGridHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtLTOutGridLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtLTInGridHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtLTInGridLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtLTMrCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "mdtDXBuildHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtDXBuildLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtDXOutGridHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtDXOutGridLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtDXInGridHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtDXInGridLow", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtDXMrCell", TextOutputFormat.class, NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "mdtBuildCellHigh", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "mdtBuildCellLow", TextOutputFormat.class, NullWritable.class, Text.class);
	}

	public static void getOutPutPackage(MultiOutputMng<NullWritable, Text> mosMng, TypeInfoMng typeInfoMng, String outpath_table, String dateStr)
	{
		String filePath = outpath_table + MDT_YD_BUILD_HIGH + dateStr;
		TypeInfo typeInfo = new TypeInfo(DataType_TB_MDTMR_BUILD_HIGH, "mdtBuildHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtBuildHigh", filePath);

		filePath = outpath_table + MDT_YD_BUILD_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_BUILD_LOW, "mdtBuildLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtBuildLow", filePath);

		filePath = outpath_table + MDT_YD_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_OUTGRID_HIGH, "mdtOutGridHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtOutGridHigh", filePath);

		filePath = outpath_table + MDT_YD_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_OUTGRID_LOW, "mdtOutGridLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtOutGridLow", filePath);

		filePath = outpath_table + MDT_YD_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_INGRID_HIGH, "mdtInGridHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtInGridHigh", filePath);

		filePath = outpath_table + MDT_YD_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_INGRID_LOW, "mdtInGridLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtInGridLow", filePath);

		filePath = outpath_table + MDT_INGRID_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_INGRID_CELL_HIGH, "mdtInGridCellHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtInGridCellHigh", filePath);

		filePath = outpath_table + MDT_INGRID_CELL_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_INGRID_CELL_LOW, "mdtInGridCellLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtInGridCellLow", filePath);

		filePath = outpath_table + MDT_OUTGRID_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_OUTGRID_CELL_HIGH, "mdtOutGridCellHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtOutGridCellHigh", filePath);

		filePath = outpath_table + MDT_OUTGRID_CELL_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_OUTGRID_CELL_LOW, "mdtOutGridCellLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtOutGridCellLow", filePath);

		filePath = outpath_table + MDT_YD_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_CELL, "mdtMrCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtMrCell", filePath);

		filePath = outpath_table + MDT_IMEI + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDT_IMEI, "mdtImei");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtImei", filePath);

		// --LT
		filePath = outpath_table + MDT_LT_BUILD_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_BUILD_HIGH, "mdtBuildHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTBuildHigh", filePath);

		filePath = outpath_table + MDT_LT_BUILD_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_BUILD_LOW, "mdtBuildLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTBuildLow", filePath);

		filePath = outpath_table + MDT_LT_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_OUTGRID_HIGH, "mdtOutGridHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTOutGridHigh", filePath);

		filePath = outpath_table + MDT_LT_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_OUTGRID_LOW, "mdtOutGridLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTOutGridLow", filePath);

		filePath = outpath_table + MDT_LT_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_INGRID_HIGH, "mdtInGridHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTInGridHigh", filePath);

		filePath = outpath_table + MDT_LT_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_INGRID_LOW, "mdtInGridLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTInGridLow", filePath);

		filePath = outpath_table + MDT_LT_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_TB_LT_MDTMR_CELL, "mdtMrCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtLTMrCell", filePath);

		// --DX
		filePath = outpath_table + MDT_DX_BUILD_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_BUILD_HIGH, "mdtBuildHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXBuildHigh", filePath);

		filePath = outpath_table + MDT_DX_BUILD_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_BUILD_LOW, "mdtBuildLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXBuildLow", filePath);

		filePath = outpath_table + MDT_DX_OUTGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_OUTGRID_HIGH, "mdtOutGridHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXOutGridHigh", filePath);

		filePath = outpath_table + MDT_DX_OUTGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_OUTGRID_LOW, "mdtOutGridLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXOutGridLow", filePath);

		filePath = outpath_table + MDT_DX_INGRID_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_INGRID_HIGH, "mdtInGridHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXInGridHigh", filePath);

		filePath = outpath_table + MDT_DX_INGRID_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_INGRID_LOW, "mdtInGridLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXInGridLow", filePath);

		filePath = outpath_table + MDT_DX_CELL + dateStr;
		typeInfo = new TypeInfo(DataType_TB_DX_MDTMR_CELL, "mdtMrCell");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtDXMrCell", filePath);
		//
		filePath = outpath_table + MDT_BUILD_CELL_HIGH + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_BUILDCELL_HIGH, "mdtBuildCellHigh");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtBuildCellHigh", filePath);

		filePath = outpath_table + MDT_BUILD_CELL_LOW + dateStr;
		typeInfo = new TypeInfo(DataType_TB_MDTMR_BUILDCELL_LOW, "mdtBuildCellLow");
		typeInfoMng.registTypeInfo(typeInfo);
		mosMng.SetOutputPath("mdtBuildCellLow", filePath);
	}
}
