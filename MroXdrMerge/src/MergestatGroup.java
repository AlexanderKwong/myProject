import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import jan.com.hadoop.hdfs.HDFSOper;
import jan.com.hadoop.mapred.DataDealJob;
import mdtstat.MdtNewTableStat;
import mergestat.ForMergeStatMain;
import mergestat.MergeDataFactory;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import mrstat.MroNewTableStat;

public class MergestatGroup
{

	public static String srcPath = "";
	public static String _successFile = "";

	public static void doMergestatGroup(String queueName, String statTime, String mroXdrMergePath, Configuration conf,
			HDFSOper hdfsOper)
	{
		srcPath = String.format("%s/mergestat/data_%s", mroXdrMergePath, statTime);
		_successFile = srcPath + "/output1/_SUCCESS";

		ArrayList<MergeInputStruct> inputPath = new ArrayList<MergeInputStruct>();

		MergeInputStruct inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_2G, String.format(
				"%1$s/xdr_loc_23g/data_%2$s/TB_2G_SIGNAL_CELL_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_SIGNAL_CELL_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_3G, String.format(
				"%1$s/xdr_loc_23g/data_%2$s/TB_3G_SIGNAL_CELL_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_SIGNAL_CELL_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_2G, String.format(
				"%1$s/xdr_loc_23g/data_%2$s/TB_2G_SIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_SIGNAL_GRID_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_3G, String.format(
				"%1$s/xdr_loc_23g/data_%2$s/TB_3G_SIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_SIGNAL_GRID_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_2G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_2G_SIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_SIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_3G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_3G_SIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_SIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_2G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_2G_DTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_DTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_3G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_3G_DTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_DTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_2G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_2G_DTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_DTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_3G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_3G_DTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_DTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_2G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_3G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_GRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_2G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_2G_CQTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct(
				"" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_3G,
				String.format(
						"%1$s/xdr_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_CELLGRID_%2$s,%1$s/mro_loc_23g/data_%2$s/TB_3G_CQTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_DTSIGNAL_GRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_DTSIGNAL_GRID_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_CQTSIGNAL_GRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_CQTSIGNAL_GRID_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_ALL_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_SIGNAL_GRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_SIGNAL_GRID_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_SIGNAL_CELL_%2$s,%1$s/xdr_loc/data_%2$s/TB_SIGNAL_CELL_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_SIGNAL_CELLGRID_%2$s,%1$s/xdr_loc/data_%2$s/TB_SIGNAL_CELLGRID_%2$s",
				mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_USERGRIDSTAT_4G, String.format(
				"%1$s/xdr_loc/data_%2$s/TB_SIGNAL_GRID_USER_HOUR_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G_FREQ, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G_FREQ, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_CQTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_FREQ, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_SIGNAL_CELL_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_USERACT_CELL, String.format(
				"%1$s/mro_loc/data_%2$s/TB_SIG_USER_BEHAVIOR_LOC_MR_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_GRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_SIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_DTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CQTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_CQTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELLGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_SIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FREQ_DTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_DTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FREQ_CQTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_CQTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_DT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_DTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_DT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_DTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_CQT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_CQTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_CQT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_CQTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_DT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_DTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_CQT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_CQTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_DT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_DTSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_CQT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_CQTSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_FREQ_GRID_BYIMEI_DT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_DTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_FREQ_GRID_BYIMEI_CQT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_CQTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_CELL_BYIMEI, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_SIGNAL_LT_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_CELL_BYIMEI, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_SIGNAL_DX_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGDTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGCQTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGFREQ_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGFREQ_CQTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGDTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGCQTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGFREQ_DTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGFREQ_CQTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_FGDTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_FGDTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_FGCQTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_FGCQTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_DT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGDTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_CQT, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGCQTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_DT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGDTSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_CQT_10, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FGCQTSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_FIGURE_CELL_BYIMEI, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_FGSIGNAL_LT_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_FIGURE_CELL_BYIMEI, String.format(
				"%1$s/mro_loc/data_%2$s/TB_FREQ_FGSIGNAL_DX_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_DT_10,
				String.format("%1$s/mro_loc/data_%2$s/TB_FREQ_FGDTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath,
						statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_CQT_10,
				String.format("%1$s/mro_loc/data_%2$s/TB_FREQ_FGCQTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath,
						statTime));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_HIGH_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_MID_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_OUTGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LOW_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_HIGH_OUT_GRID_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_MID_OUT_GRID_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_OUTGRID_CELL_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LOW_OUT_GRID_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_HIGH_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_MID_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_INGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LOW_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_HIGH_IN_GRID_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_MID_IN_GRID_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_INGRID_CELL_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LOW_IN_GRID_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_HIGH_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_BUILD_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_MID_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_BUILD_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LOW_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_BUILD_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_HIGH_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_MID_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_OUTGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_LOW_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_HIGH_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_MID_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_INGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_LOW_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_HIGH_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_BUILD_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_MID_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_BUILD_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_LOW_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_BUILD_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_DX_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_HIGH_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_MID_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_OUTGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_LOW_OUT_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_HIGH_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_MID_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_INGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_LOW_IN_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_HIGH_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_BUILD_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_MID_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_BUILD_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_LOW_BUILD, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_BUILD_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_LT_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MR_CELLBUILD_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_BUILD_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MR_CELLBUILD_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_BUILD_CELL_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MR_CELLBUILD_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_BUILD_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.AREA_CELL_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YD_AREA_GRID_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.AREA_CELL, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MroNewTableStat.MRO_YD_AREA_CELL + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.AREA_GRID, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MroNewTableStat.MRO_YD_AREA_GRID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.AREA, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MroNewTableStat.MRO_YD_AREA + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_BUILD_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_YD_BUILD_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_BUILD_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_YD_BUILD_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_INGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_YD_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_INGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_YD_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_INGRID_CELL_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_INGRID_CELL_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_CELL_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_CELL_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_CELL, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MdtNewTableStat.MDT_YD_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDT_IMEI, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MdtNewTableStat.MDT_IMEI + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_BUILD_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_BUILD_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_BUILD_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_BUILD_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_OUTGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_OUTGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_INGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_INGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_MDTMR_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_LT_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_BUILD_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_BUILD_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_BUILD_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_BUILD_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_OUTGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_OUTGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_INGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_INGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_MDTMR_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_DX_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_BUILDCELL_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_BUILD_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_MDTMR_BUILDCELL_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MdtNewTableStat.MDT_BUILD_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		// yzx add LT and DX AREA 2017.9.18
		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_AREA_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_AREA_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_AREA_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_LT_AREA_GRID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_LT_AREA, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MroNewTableStat.MRO_LT_AREA + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_AREA_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_AREA_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_AREA_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_DX_AREA_GRID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_DX_AREA, String.format("%1$s/mro_loc/data_01_%2$s"
				+ MroNewTableStat.MRO_DX_AREA + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		// yzx add EVENT 2017.9.26
		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_CELL, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_INGRID_CELL_HIGH, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_INGRID_CELL_MID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_CELL_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_INGRID_CELL_LOW, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_INGRID_HIGH, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_INGRID_MID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_INGRID_LOW, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_CELL_HIGH, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_CELL_MID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_CELL_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_CELL_LOW, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_HIGH, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_MID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_LOW, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_CELL_HIGH, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_BUILD_GRID_CELL_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_CELL_MID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_BUILD_GRID_CELL_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_CELL_LOW, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_BUILD_GRID_CELL_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_HIGH, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_BUILD_GRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_MID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_BUILD_GRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_LOW, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_BUILD_GRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		// EVENT_AREA
		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_AREA, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_AREA_GRID, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_AREA_GRID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_AREA_GRID_CELL, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_AREA_GRID_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.TB_EVENT_AREA_CELL, String.format(
				"%1$s/xdr_locall/data_01_%2$s" + MroNewTableStat.EVENT_AREA_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		// 20170921 add fullnet
		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_OUTGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_OUTGRID_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_OUTGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_OUTGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_OUTGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_OUTGRID_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_OUTGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_OUTGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_INGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_INGRID_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_INGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_INGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_INGRID_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_INGRID_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_INGRID_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_INGRID_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_INGRID_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_INGRID_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_BUILDING_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_BUILDING_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_BUILDING_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_BUILDING_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_BUILDING_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_BUILDING_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_BUILDING_HIGH, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_BUILDING_HIGH + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_BUILDING_MID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_BUILDING_MID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_BUILDING_LOW, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_BUILDING_LOW + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_AREA_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_AREA_GRID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_AREA_GRID, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_AREA_GRID + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_AREA_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_AREA_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_AREA_CELL, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_AREA_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDLT_AREA, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDLT_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MergeDataFactory.MERGETYPE_YDDX_AREA, String.format(
				"%1$s/mro_loc/data_01_%2$s" + MroNewTableStat.MRO_YDDX_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		inputPath.add(inputInfo);

		// /////////////////////////////////////////// output
		// ///////////////////////////////////////////
		// //////////////////////////////////////////////////
		ArrayList<MergeOutPutStruct> outputList = new ArrayList<MergeOutPutStruct>();

		MergeOutPutStruct outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_2G,
				String.format("cell2g"), String.format("%1$s/mergestat/data_%2$s/TB_2G_SIGNAL_CELL_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_3G, String.format("cell3g"),
				String.format("%1$s/mergestat/data_%2$s/TB_3G_SIGNAL_CELL_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_2G, String.format("grid2g"),
				String.format("%1$s/mergestat/data_%2$s/TB_2G_SIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_3G, String.format("grid3g"),
				String.format("%1$s/mergestat/data_%2$s/TB_3G_SIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_2G,
				String.format("cellgrid2g"), String.format("%1$s/mergestat/data_%2$s/TB_2G_SIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_3G,
				String.format("cellgrid3g"), String.format("%1$s/mergestat/data_%2$s/TB_3G_SIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_2G, String.format("griddt2g"),
				String.format("%1$s/mergestat/data_%2$s/TB_2G_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_3G, String.format("griddt3g"),
				String.format("%1$s/mergestat/data_%2$s/TB_3G_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_2G,
				String.format("cellgriddt2g"), String.format("%1$s/mergestat/data_%2$s/TB_2G_DTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_DT_3G,
				String.format("cellgriddt3g"), String.format("%1$s/mergestat/data_%2$s/TB_3G_DTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_2G,
				String.format("gridcqt2g"), String.format("%1$s/mergestat/data_%2$s/TB_2G_CQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_3G,
				String.format("gridcqt3g"), String.format("%1$s/mergestat/data_%2$s/TB_3G_CQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_2G,
				String.format("cellgridcqt2g"), String.format("%1$s/mergestat/data_%2$s/TB_2G_CQTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_CQT_3G,
				String.format("cellgridcqt3g"), String.format("%1$s/mergestat/data_%2$s/TB_3G_CQTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G, String.format("griddt4g"),
				String.format("%1$s/mergestat/data_%2$s/TB_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G,
				String.format("gridcqt4g"), String.format("%1$s/mergestat/data_%2$s/TB_CQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_ALL_4G, String.format("grid4g"),
				String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_4G, String.format("cell4g"),
				String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_CELL_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRIDSTAT_4G,
				String.format("cellgrid4g"), String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_USERGRIDSTAT_4G,
				String.format("usergrid4g"), String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_GRID_USER_HOUR_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_DT_4G_FREQ,
				String.format("griddt4gfreq"), String.format("%1$s/mergestat/data_%2$s/TB_FREQ_DTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRIDSTAT_CQT_4G_FREQ,
				String.format("gridcqt4gfreq"), String.format("%1$s/mergestat/data_%2$s/TB_FREQ_CQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLSTAT_FREQ,
				String.format("cellgridfreq"), String.format("%1$s/mergestat/data_%2$s/TB_FREQ_SIGNAL_CELL_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_USERACT_CELL,
				String.format("useractcell"), String.format(
						"%1$s/mergestat/data_%2$s/TB_SIG_USER_BEHAVIOR_LOC_MR_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_GRID_4G_10, String.format("grid10"),
				String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DTGRID_4G_10, String.format("dtgrid10"),
				String.format("%1$s/mergestat/data_%2$s/TB_DTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CQTGRID_4G_10, String.format("cqtgrid10"),
				String.format("%1$s/mergestat/data_%2$s/TB_CQTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELLGRID_4G_10,
				String.format("cellgrid10"), String.format("%1$s/mergestat/data_%2$s/TB_SIGNAL_CELLGRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FREQ_DTGRID_4G_10,
				String.format("freqdtgrid10"), String.format("%1$s/mergestat/data_%2$s/TB_FREQ_DTSIGNAL_GRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FREQ_CQTGRID_4G_10,
				String.format("freqcqtgrid10"), String.format("%1$s/mergestat/data_%2$s/TB_FREQ_CQTSIGNAL_GRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_DT,
				String.format("freqGridDt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_DTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_DT_10,
				String.format("freqGridDt10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_DTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_CQT,
				String.format("freqGridCqt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_CQTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FREQ_GRID_BYIMEI_CQT_10,
				String.format("freqGridCqt10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_CQTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_DT, String.format("cellgriddt"),
				String.format("%1$s/mergestat/data_%2$s/TB_DTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_CQT,
				String.format("cellgridcqt"), String.format("%1$s/mergestat/data_%2$s/TB_CQTSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_DT_10,
				String.format("tencellgriddt"), String.format("%1$s/mergestat/data_%2$s/TB_DTSIGNAL_CELLGRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELL_GRID_CQT_10,
				String.format("tencellgridcqt"), String.format("%1$s/mergestat/data_%2$s/TB_CQTSIGNAL_CELLGRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_FREQ_GRID_BYIMEI_DT_10,
				String.format("dxFreqGridDt10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_DTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_FREQ_GRID_BYIMEI_CQT_10,
				String.format("dxFreqGridCqt10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_CQTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_CELL_BYIMEI,
				String.format("ltfreqcell"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_SIGNAL_LT_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_CELL_BYIMEI,
				String.format("dxfreqcell"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_SIGNAL_DX_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G,
				String.format("figuregrid"), String.format("%1$s/mergestat/data_%2$s/TB_FGSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G,
				String.format("figuredtgrid"), String.format("%1$s/mergestat/data_%2$s/TB_FGDTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G,
				String.format("figurecqtgrid"), String.format("%1$s/mergestat/data_%2$s/TB_FGCQTSIGNAL_GRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G,
				String.format("figurecellgrid"), String.format("%1$s/mergestat/data_%2$s/TB_FGSIGNAL_CELLGRID_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G,
				String.format("figurefreqdtgrid"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGFREQ_DTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G,
				String.format("figurefreqcqtgrid"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGFREQ_CQTSIGNAL_GRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_GRID_4G_10,
				String.format("figuregrid10"), String.format("%1$s/mergestat/data_%2$s/TB_FGSIGNAL_GRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_DTGRID_4G_10,
				String.format("figuredtgrid10"), String.format("%1$s/mergestat/data_%2$s/TB_FGDTSIGNAL_GRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CQTGRID_4G_10,
				String.format("figurecqtgrid10"), String.format("%1$s/mergestat/data_%2$s/TB_FGCQTSIGNAL_GRID10_%2$s",
						mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELLGRID_4G_10,
				String.format("figurecellgrid10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_DTGRID_4G_10,
				String.format("figurefreqdtgrid10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGFREQ_DTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_CQTGRID_4G_10,
				String.format("figurefreqcqtgrid10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGFREQ_CQTSIGNAL_GRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT,
				String.format("figurefreqGridDt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_FGDTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_DT_10,
				String.format("figurefreqGridDt10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_FGDTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT,
				String.format("figurefreqGridCqt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_FGCQTSIGNAL_LT_GRID_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_FREQ_GRID_BYIMEI_CQT_10,
				String.format("figurefreqGridCqt10"),
				String.format("%1$s/mergestat/data_%2$s/TB_FREQ_FGCQTSIGNAL_LT_GRID10_BYIMEI_%2$s", mroXdrMergePath,
						statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_DT,
				String.format("figurecellgriddt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGDTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_CQT,
				String.format("figurecellgridcqt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGCQTSIGNAL_CELLGRID_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_DT_10,
				String.format("figuretencellgriddt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGDTSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_FIGURE_CELL_GRID_CQT_10,
				String.format("figuretencellgridcqt"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FGCQTSIGNAL_CELLGRID10_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_FIGURE_CELL_BYIMEI,
				String.format("fgltfreqcell"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_FGSIGNAL_LT_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_FIGURE_CELL_BYIMEI,
				String.format("fgdxfreqcell"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_FGSIGNAL_DX_CELL_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_DT_10,
				String.format("figuredxFreqGridDT10"), String.format(
						"%1$s/mergestat/data_%2$s/TB_FREQ_FGDTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath, statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_FIGURE_FREQ_GRID_BYIMEI_CQT_10,
				String.format("figuredxFreqGridCqt10"),
				String.format("%1$s/mergestat/data_%2$s/TB_FREQ_FGCQTSIGNAL_DX_GRID10_BYIMEI_%2$s", mroXdrMergePath,
						statTime));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_HIGH_OUT_GRID,
				String.format("mrhighoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_MID_OUT_GRID,
				String.format("mrmidoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_YD_OUTGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LOW_OUT_GRID,
				String.format("mrlowoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_HIGH_OUT_GRID_CELL,
				String.format("mrhighoutgridcell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_MID_OUT_GRID_CELL,
				String.format("mrmidoutgridcell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_OUTGRID_CELL_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LOW_OUT_GRID_CELL,
				String.format("mrlowoutgridcell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_HIGH_IN_GRID,
				String.format("mrhighingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_YD_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_MID_IN_GRID, String.format("mrmidingrid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_INGRID_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LOW_IN_GRID, String.format("mrlowingrid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_INGRID_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_HIGH_IN_GRID_CELL,
				String.format("mrhighingridcell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_MID_IN_GRID_CELL,
				String.format("mrmidingridcell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_INGRID_CELL_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LOW_IN_GRID_CELL,
				String.format("mrlowingridcell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_INGRID_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_HIGH_BUILD, String.format("mrhighbuild"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_BUILD_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_MID_BUILD, String.format("mrmidbuild"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_BUILD_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LOW_BUILD, String.format("mrlowbuild"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_BUILD_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_CELL, String.format("mrcell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_CELL + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_HIGH_OUT_GRID,
				String.format("mrdxhighoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_MID_OUT_GRID,
				String.format("mrdxmidoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_OUTGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_LOW_OUT_GRID,
				String.format("mrdxlowoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_HIGH_IN_GRID,
				String.format("mrdxhighingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_MID_IN_GRID,
				String.format("mrdxmidingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_INGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_LOW_IN_GRID,
				String.format("mrdxlowingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_INGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_HIGH_BUILD,
				String.format("mrdxhighbuild"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_BUILD_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_MID_BUILD,
				String.format("mrdxmidbuild"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_BUILD_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_LOW_BUILD,
				String.format("mrdxlowbuild"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_DX_BUILD_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_DX_CELL, String.format("mrdxcell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_DX_CELL + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_HIGH_OUT_GRID,
				String.format("mrlthighoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_MID_OUT_GRID,
				String.format("mrltmidoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_OUTGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_LOW_OUT_GRID,
				String.format("mrltlowoutgrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_HIGH_IN_GRID,
				String.format("mrlthighingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_MID_IN_GRID,
				String.format("mrltmidingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_INGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_LOW_IN_GRID,
				String.format("mrltlowingrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_INGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_HIGH_BUILD,
				String.format("mrlthighbuild"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_BUILD_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_MID_BUILD,
				String.format("mrltmidbuild"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_BUILD_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_LOW_BUILD,
				String.format("mrltlowbuild"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_LT_BUILD_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_LT_CELL, String.format("mrltcell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_LT_CELL + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MR_CELLBUILD_HIGH,
				String.format("mrcellbuildhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_BUILD_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MR_CELLBUILD_MID,
				String.format("mrcellbuildmid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_BUILD_CELL_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MR_CELLBUILD_LOW,
				String.format("mrcellbuildlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.MRO_BUILD_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.AREA_CELL_GRID, String.format("scenecellgrid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_AREA_GRID_CELL + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.AREA_CELL, String.format("scenecell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_AREA_CELL + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.AREA_GRID, String.format("scenegrid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_AREA_GRID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.AREA, String.format("scene"), String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YD_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_BUILD_HIGH, String.format("mdtbuildhigh"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_YD_BUILD_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_BUILD_LOW, String.format("mdtbuildlow"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_YD_BUILD_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_HIGH,
				String.format("mdtoutgridhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_LOW,
				String.format("mdtoutgridlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_INGRID_HIGH,
				String.format("mdtingridhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_YD_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_INGRID_LOW, String.format("mdtingridlow"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_YD_INGRID_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_INGRID_CELL_HIGH,
				String.format("mdtingridcellhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_INGRID_CELL_LOW,
				String.format("mdtingridcelllow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_INGRID_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_CELL_HIGH,
				String.format("mdtoutgridcellhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_OUTGRID_CELL_LOW,
				String.format("mdtoutgridcelllow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_CELL, String.format("mdtcell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_YD_CELL_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDT_IMEI, String.format("mdtimei"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_IMEI + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_BUILD_HIGH,
				String.format("ltmdtbuildhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_LT_BUILD_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_BUILD_LOW,
				String.format("ltmdtbuildlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_LT_BUILD_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_OUTGRID_HIGH,
				String.format("ltmdtoutgridhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_OUTGRID_LOW,
				String.format("ltmdtoutgridlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_INGRID_HIGH,
				String.format("ltmdtingridhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_LT_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_INGRID_LOW,
				String.format("ltmdtingridlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_LT_INGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_MDTMR_CELL, String.format("ltmdtcell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_LT_CELL + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_BUILD_HIGH,
				String.format("dxmdtbuildhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_DX_BUILD_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_BUILD_LOW,
				String.format("dxmdtbuildlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_DX_BUILD_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_OUTGRID_HIGH,
				String.format("dxmdtoutgridhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_OUTGRID_LOW,
				String.format("dxmdtoutgridlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_INGRID_HIGH,
				String.format("dxmdtingirdhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_DX_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_INGRID_LOW,
				String.format("dxmdtingirdlow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_DX_INGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_MDTMR_CELL, String.format("dxmdtcell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MdtNewTableStat.MDT_DX_CELL + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_BUILDCELL_HIGH,
				String.format("dxmdtbuildcellhigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_BUILD_CELL_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_MDTMR_BUILDCELL_LOW,
				String.format("dxmdtbuildcelllow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MdtNewTableStat.MDT_BUILD_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		// yzx add LT and DX AREA 2017.9.18
		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_AREA_CELL, String.format("ltscenecell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_LT_AREA_CELL + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_AREA_GRID, String.format("ltscenegrid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_LT_AREA_GRID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_LT_AREA, String.format("ltscene"), String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_LT_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_AREA_CELL, String.format("dxscenecell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_DX_AREA_CELL + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_AREA_GRID, String.format("dxscenegrid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_DX_AREA_GRID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_DX_AREA, String.format("dxscene"), String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_DX_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		// yzx add EVENT 2017.9.26
		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_CELL, String.format("tbEventCell"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.EVENT_CELL + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_INGRID_CELL_HIGH,
				String.format("tbEventIngridCellHigh"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.EVENT_INGRID_CELL_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_INGRID_CELL_MID,
				String.format("tbEventIngridCellMid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_INGRID_CELL_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_INGRID_CELL_LOW,
				String.format("tbEventIngridCellLow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_INGRID_CELL_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_INGRID_HIGH,
				String.format("tbEventIngridHigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_INGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_INGRID_MID,
				String.format("tbEventIngridMid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_INGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_INGRID_LOW,
				String.format("tbEventIngridLow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_INGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_CELL_HIGH,
				String.format("tbEventOutgridCellHigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_CELL_MID,
				String.format("tbEventOutgridCellMid"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_CELL_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_CELL_LOW,
				String.format("tbEventOutgridCellLow"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.EVENT_OUTGRID_CELL_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_HIGH,
				String.format("tbEventOutgridHigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_OUTGRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_MID,
				String.format("tbEventOutgridMid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_OUTGRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_OUTGRID_LOW,
				String.format("tbEventOutgridLow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_OUTGRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_CELL_HIGH,
				String.format("tbEventBuildGridCellHigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_BUILD_GRID_CELL_HIGH + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_CELL_MID,
				String.format("tbEventBuildGridCellMid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_BUILD_GRID_CELL_MID + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_CELL_LOW,
				String.format("tbEventBuildGridCellLow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_BUILD_GRID_CELL_LOW + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_HIGH,
				String.format("tbEventBuildGridHigh"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_BUILD_GRID_HIGH + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_MID,
				String.format("tbEventBuildGridMid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_BUILD_GRID_MID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_BUILD_GRID_LOW,
				String.format("tbEventBuildGridLow"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_BUILD_GRID_LOW + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		// EVENT_AREA
		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_AREA, String.format("tbEventArea"),
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.EVENT_AREA + "%2$s", mroXdrMergePath,
						statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_AREA_GRID,
				String.format("tbEventAreaGrid"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_AREA_GRID + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_AREA_GRID_CELL,
				String.format("tbEventAreaGridCell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_AREA_GRID_CELL + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.TB_EVENT_AREA_CELL,
				String.format("tbEventAreaCell"), String.format("%1$s/mergestat/data_01_%2$s"
						+ MroNewTableStat.EVENT_AREA_CELL + "%2$s", mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		// 20170921 add fullnet
		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_OUTGRID_HIGH, "ydlthighoutgrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_OUTGRID_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_OUTGRID_MID, "ydltmidoutgrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_OUTGRID_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_OUTGRID_LOW, "ydltlowoutgrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_OUTGRID_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_OUTGRID_HIGH, "yddxhighoutgrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_OUTGRID_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_OUTGRID_MID, "yddxmidoutgrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_OUTGRID_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_OUTGRID_LOW, "yddxlowoutgrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_OUTGRID_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_INGRID_HIGH, "ydlthighingrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_INGRID_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_INGRID_MID, "ydltmidingrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_INGRID_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_INGRID_LOW, "ydltlowingrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_INGRID_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_INGRID_HIGH, "yddxhighingrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_INGRID_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_INGRID_MID, "yddxmidingrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_INGRID_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_INGRID_LOW, "yddxlowingrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_INGRID_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_BUILDING_HIGH, "ydlthighbuilding",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_BUILDING_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_BUILDING_MID, "ydltmidbuilding",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_BUILDING_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_BUILDING_LOW, "ydltlowbuilding",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_BUILDING_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_BUILDING_HIGH, "yddxhighbuilding",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_BUILDING_HIGH + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_BUILDING_MID, "yddxmidbuilding",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_BUILDING_MID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_BUILDING_LOW, "yddxlowbuilding",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_BUILDING_LOW + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_CELL, "ydltcell", String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_CELL, "yddxcell", String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_CELL + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_AREA_GRID, "ydltareagrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_AREA_GRID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_AREA_GRID, "yddxareagrid",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_AREA_GRID + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_AREA_CELL, "ydltareacell",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_AREA_CELL + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_AREA_CELL, "yddxareacell",
				String.format("%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_AREA_CELL + "%2$s",
						mroXdrMergePath, statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDLT_AREA, "ydltarea", String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDLT_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeDataFactory.MERGETYPE_YDDX_AREA, "yddxarea", String.format(
				"%1$s/mergestat/data_01_%2$s" + MroNewTableStat.MRO_YDDX_AREA + "%2$s", mroXdrMergePath,
				statTime.replace("01_", "")));
		outputList.add(outputStruct);

		long fileCount = 0L;
		for (int i = (inputPath.size() - 1); i >= 0; i--)
		{
			long tempFileCount = hdfsOper.getFileCount(inputPath.get(i).inputPath);
			if (tempFileCount <= 0)// 去掉不存在的目录
			{
				inputPath.remove(i);
				outputList.remove(i);
			}
			else
			{
				fileCount = fileCount + tempFileCount;
			}
		}
		int groupCount = fileCount % 100000 == 0 ? (int) (fileCount / 100000) : (int) (fileCount / 100000 + 1);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan))
		{
			groupCount = fileCount % 50000 == 0 ? (int) (fileCount / 50000) : (int) (fileCount / 50000 + 1);
		}
		if (groupCount > inputPath.size())
		{
			groupCount = inputPath.size();
		}
		int mergePackageNum = inputPath.size() % groupCount == 0 ? (inputPath.size() / groupCount) : (inputPath.size() / groupCount + 1);// 合并文件夹个数
		System.out.println("total  inputPackage num : " + inputPath.size());
		System.out.println("total  file num : " + fileCount);
		System.out.println("merged times:" + groupCount);
		int beginIndex = 0;
		int length = 0;

		if (mroXdrMergePath.contains(":") || !hdfsOper.checkFileExist(_successFile))
		{
			for (int i = 1; i <= groupCount; i++)
			{
				if (i == groupCount)// 最后一组汇聚文件夹个数少于mergePackageNum
				{
					beginIndex = mergePackageNum * (groupCount - 1);
					length = inputPath.size() - beginIndex;
				}
				else
				{
					beginIndex = (i - 1) * mergePackageNum;
					length = mergePackageNum;
				}
				ArrayList<String> paramsList = new ArrayList<String>();
				paramsList.add("100");
				paramsList.add(queueName);
				paramsList.add(statTime);
				paramsList.add(srcPath);
				paramsList.add(length + "");
				fillInPutArray(beginIndex, length, inputPath, paramsList);
				paramsList.add(length + "");
				fillOutPutArray(beginIndex, length, outputList, paramsList);
				String[] params = listToArray(paramsList);
				for (String temp : params)
				{
					System.out.println("[" + temp + "]");
				}
				mergeDo(mroXdrMergePath, hdfsOper, srcPath, conf, params, i);
			}
			hdfsOper.mkfile(String.format("%1$s/mergestat/data_%2$s/Finished", mroXdrMergePath, statTime));
		}

	}

	public static void mergeDo(String mroXdrMergePath, HDFSOper hdfsOper, String successPath, Configuration conf,
			String[] params, int roundId)
	{
		try
		{
			System.out.println("the [" + roundId + "] times merge");
			Job curJob;
			curJob = ForMergeStatMain.CreateJob(conf, params, roundId);
			DataDealJob dataJob = new DataDealJob(curJob, hdfsOper);
			if (!dataJob.Work())
			{
				System.out.println("mergestat job error! stop run.");
				throw (new Exception("system.exit1"));
			}
			else
			{
				System.out.println("the[" + roundId + "] times has been dealed succesfully once");
			}
		}
		catch (Exception e)
		{
		}
	}

	public static void fillInPutArray(int begianIndex, int length, ArrayList<MergeInputStruct> srcArray,
			ArrayList<String> paramsList)
	{
		for (int i = 0; i < length; i++)
		{
			paramsList.add(srcArray.get(begianIndex + i).index);
			paramsList.add(srcArray.get(begianIndex + i).inputPath);
		}
	}

	public static void fillOutPutArray(int begianIndex, int length, ArrayList<MergeOutPutStruct> srcArray,
			ArrayList<String> paramsList)
	{
		for (int i = 0; i < length; i++)
		{
			paramsList.add(srcArray.get(begianIndex + i).index);
			paramsList.add(srcArray.get(begianIndex + i).fileName);
			paramsList.add(srcArray.get(begianIndex + i).outpath);
		}
	}

	public static String[] listToArray(ArrayList<String> list)
	{
		String[] params = new String[list.size()];
		for (int i = 0; i < list.size(); i++)
		{
			params[i] = list.get(i);
		}
		return params;
	}
}
