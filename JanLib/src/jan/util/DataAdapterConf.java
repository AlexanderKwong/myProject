package jan.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DataAdapterConf
{
	private Map<String, ParseItem> parseItemMap = null;

	public DataAdapterConf()
	{
		parseItemMap = new HashMap<String, ParseItem>();
	}

	public boolean init(String confPath) throws IOException
	{
		try
		{
			FileReader reader = new FileReader(confPath);
			BufferedReader br = new BufferedReader(reader);
			String str = "";
			while ((str = br.readLine()) != null)
			{
				str = str.trim();
				parseData(str);
			}
			br.close();
			reader.close();
		}
		catch (IOException e)
		{
			throw e;
		}

		return true;
	}

	public boolean init(InputStream inputStream) throws IOException
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
		String str = "";
		while ((str = br.readLine()) != null)
		{
			str = str.trim();
			parseData(str);
		}
		return true;
	}

	private int curPos = 0;
	private ParseItem curParseItem = null;

	private void parseData(String parseStr)
	{
		if (parseStr.length() == 0)
		{
			return;
		}

		if (parseStr.length() >= 2 && parseStr.substring(0, 2).equals("##"))
		{
			return;
		}

		if (curPos == 0 && parseStr.indexOf("#TYPENAME:") >= 0)
		{
			String typeName = parseStr.substring(parseStr.indexOf(":") + 1);
			curParseItem = parseItemMap.get(typeName);
			if (curParseItem == null)
			{
				curParseItem = new ParseItem(typeName);
				parseItemMap.put(typeName, curParseItem);
			}
			curPos = 1;
		}
		else if (parseStr.indexOf("#SPLIT:") >= 0)
		{
			String mark = parseStr.substring(parseStr.indexOf(":") + 1);
			if (mark.indexOf("\\\\") >= 0)
			{
				mark = mark.replace("\\\\", "\\");
			}
			else if (mark.indexOf("\\t") >= 0)
			{
				mark = mark.replace("\\t", "\t");
			}
			curParseItem.setSplitMark(mark);
		}
		else if (parseStr.equals("#BEGIN"))
		{
			if (curPos == 1)
			{
				curPos = 2;
			}
		}
		else if (parseStr.equals("#END"))
		{
			if (curPos == 2)
			{
				curPos = 0;
			}
		}
		else
		{
			if (curPos == 2)
			{
				if (!parseStr.substring(0, 1).equals("#"))
				{
					String[] strs = parseStr.split(";");

					String[] posStrs = strs[0].split("=");
					if (strs.length == 1)
					{
						curParseItem.addColum(posStrs[0].trim(), Integer.parseInt(posStrs[1].trim()), "");
					}
					else if (strs.length == 2)
					{
						curParseItem.addColum(posStrs[0].trim(), Integer.parseInt(posStrs[1].trim()), strs[1]);
					}
				}
			}
		}

	}

	public ParseItem getParseItem(String typeName)
	{
		ParseItem parseItem = parseItemMap.get(typeName);
		return parseItem;
	}

	public class ParseItem
	{
		private String parseType = "";
		private Map<String, ColumnInfo> columPosMap;
		private String splitMark = "\t";
		private int splitSize;
		private String formatFunc = "";

		public ParseItem(String parseType)
		{
			this.parseType = parseType;
			splitSize = -1;
			columPosMap = new HashMap<String, ColumnInfo>();
		}

		public String getParseType()
		{
			return parseType;
		}

		public Map<String, ColumnInfo> getColumPosMap()
		{
			return columPosMap;
		}

		/**
		 * 更换位置
		 * 
		 * @param ColuName
		 * @param pos
		 */
		public void setPos(String ColuName, int pos)
		{
			ColumnInfo colNum = columPosMap.get(ColuName);
			if (colNum != null)
			{
				colNum.pos = pos;
			}
		}

		public String getSplitMark()
		{
			return splitMark;
		}

		public int getSplitSize()
		{
			return splitSize;
		}

		public String getFormatFunc()
		{
			return formatFunc;
		}

		public void setSplitMark(String splitMark)
		{
			this.splitMark = splitMark;
		}

		public void addColum(String name, int pos, String formatFunc)
		{
			if (pos < 0)
			{
				return;
			}
			splitSize = splitSize > pos + 2 ? splitSize : pos + 2;

			ColumnInfo columnItem = new ColumnInfo();
			columnItem.columnName = name;
			columnItem.pos = pos;
			columnItem.formatFunc = formatFunc;
			columPosMap.put(name, columnItem);
		}

		public ColumnInfo getColumInfo(String columnName)
		{
			if (columPosMap.containsKey(columnName))
			{
				return columPosMap.get(columnName);
			}
			return null;
		}

		public int getSplitMax(String columns)
		{
			int splitMax = -1;
			String[] strs = columns.split(",");
			for (String str : strs)
			{
				ColumnInfo ci = getColumInfo(str);
				if (ci != null)
				{
					splitMax = Math.max(ci.pos, splitMax);
				}
			}
			return splitMax;
		}

	}

	public class ColumnInfo
	{
		public String columnName;
		public int pos;
		public String formatFunc;

		public ColumnInfo()
		{
			columnName = "";
			pos = -1;
			formatFunc = "";
		}

		@Override
		public String toString()
		{
			return "ColumnInfo [columnName=" + columnName + ", pos=" + pos + ", formatFunc=" + formatFunc + "]";
		}

	}

}
