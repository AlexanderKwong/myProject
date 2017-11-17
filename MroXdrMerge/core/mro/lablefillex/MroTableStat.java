package mro.lablefillex;

import org.apache.hadoop.io.Text;

import mro.lablefill.XdrLable;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;

public class MroTableStat
{
	public void dealMro(Iterable<Text> values)
	{

	}

	public void dealXdrLocation(String xdrlocation)
	{
		String[] strs = xdrlocation.split("\t", -1);
		for (int i = 0; i < strs.length; ++i)
		{
			strs[i] = strs[i].trim();
		}
	}
}
