package util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import cellconfig.CellConfig;
import cellconfig.FilterCellConfig;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class FilterByEci
{
	public static void readEciList(Configuration conf) throws IOException
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.FilterCell))
		{
			if (!FilterCellConfig.GetInstance().loadFilterCell(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "filtercell init error please check！");
				throw (new IOException("filtercell init error please check！"));
			}
			else
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "filtercell count:" + FilterCellConfig.GetInstance().getLteCellInfoList().size());
			}
		}
	}

	public static boolean ifMap(long eci)
	{
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.FilterCell))
		{
			return FilterCellConfig.GetInstance().getLteCell(eci);
		}
		return true;
	}
}
