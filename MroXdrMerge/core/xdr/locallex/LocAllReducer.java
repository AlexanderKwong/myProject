package xdr.locallex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import StructData.StaticConfig;
import cellconfig.CellConfig;
import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.LOGHelper;
import jan.util.StringHelper;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.locallex.model.StatVideoLag;
import xdr.locallex.model.XdrDataBase;
import xdr.locallex.model.XdrDataFactory;

public class LocAllReducer
{

	public static class StatReducer extends DataDealReducer<ImsiKey, Text, NullWritable, Text>
	{
		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();
		private StringBuffer tmsb = new StringBuffer();

		private String[] strs;
		private Map<Integer, String> outpathIndexMap;
		private Map<Integer, String> outtypeIndexMap;

		private ParseItem curParseItem = null;
		private DataAdapterReader curDataAdapterReader = null;
		private int splitMax = -1;
		private LocItemMng locItemMng;
		private ImsiKey curImsiKey = null;
		private DataStater dataStater = null;

		// table
		private String path_TB_EVENT_MID_IN_SAMPLE;
		private String path_TB_EVENT_MID_OUT_SAMPLE;
		private String path_TB_EVENT_LOW_IN_SAMPLE;
		private String path_TB_EVENT_LOW_OUT_SAMPLE;
		private String path_TB_EVENT_HIGH_IN_SAMPLE;
		private String path_TB_EVENT_HIGH_OUT_SAMPLE;

		private String path_TB_EVENT_HIGH_OUT_GRID;
		private String path_TB_EVENT_MID_OUT_GRID;
		private String path_TB_EVENT_LOW_OUT_GRID;

		private String path_TB_EVENT_HIGH_OUT_CELLGRID;
		private String path_TB_EVENT_MID_OUT_CELLGRID;
		private String path_TB_EVENT_LOW_OUT_CELLGRID;

		private String path_TB_EVENT_HIGH_IN_GRID;
		private String path_TB_EVENT_MID_IN_GRID;
		private String path_TB_EVENT_LOW_IN_GRID;

		private String path_TB_EVENT_HIGH_IN_CELLGRID;
		private String path_TB_EVENT_MID_IN_CELLGRID;
		private String path_TB_EVENT_LOW_IN_CELLGRID;

		// build
		private String path_TB_EVENT_HIGH_BUILD_GRID;
		private String path_TB_EVENT_MID_BUILD_GRID;
		private String path_TB_EVENT_LOW_BUILD_GRID;

		private String path_TB_EVENT_HIGH_BUILD_CELLGRID;
		private String path_TB_EVENT_MID_BUILD_CELLGRID;
		private String path_TB_EVENT_LOW_BUILD_CELLGRID;

		private String path_TB_EVENT_CELL;

		// 高铁场景
		private String path_TB_EVENT_AREA;
		private String path_TB_EVENT_AREA_CELL;
		private String path_TB_EVENT_AREA_CELLGRID;
		private String path_TB_EVENT_AREA_GRID;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);

			// table
			path_TB_EVENT_MID_IN_SAMPLE = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_MID_IN_SAMPLE");
			path_TB_EVENT_MID_OUT_SAMPLE = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_MID_OUT_DTSAMPLE");
			path_TB_EVENT_LOW_IN_SAMPLE = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_LOW_IN_SAMPLE");
			path_TB_EVENT_LOW_OUT_SAMPLE = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_LOW_OUT_SAMPLE");
			path_TB_EVENT_HIGH_IN_SAMPLE = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_HIGH_IN_SAMPLE");
			path_TB_EVENT_HIGH_OUT_SAMPLE = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_HIGH_OUT_DTSAMPLE");

			path_TB_EVENT_HIGH_OUT_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_OUT_GRID");
			path_TB_EVENT_MID_OUT_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_OUT_GRID");
			path_TB_EVENT_LOW_OUT_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_OUT_GRID");

			path_TB_EVENT_HIGH_OUT_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_OUT_CELLGRID");
			path_TB_EVENT_MID_OUT_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_OUT_CELLGRID");
			path_TB_EVENT_LOW_OUT_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_OUT_CELLGRID");

			path_TB_EVENT_HIGH_IN_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_IN_GRID");
			path_TB_EVENT_MID_IN_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_IN_GRID");
			path_TB_EVENT_LOW_IN_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_IN_GRID");

			path_TB_EVENT_HIGH_IN_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_IN_CELLGRID");
			path_TB_EVENT_MID_IN_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_IN_CELLGRID");
			path_TB_EVENT_LOW_IN_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_IN_CELLGRID");

			// build
			path_TB_EVENT_HIGH_BUILD_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_BUILD_GRID");
			path_TB_EVENT_MID_BUILD_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_BUILD_GRID");
			path_TB_EVENT_LOW_BUILD_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_BUILD_GRID");

			path_TB_EVENT_HIGH_BUILD_CELLGRID = conf
					.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_HIGH_BUILD_CELLGRID");
			path_TB_EVENT_MID_BUILD_CELLGRID = conf
					.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_MID_BUILD_CELLGRID");
			path_TB_EVENT_LOW_BUILD_CELLGRID = conf
					.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_LOW_BUILD_CELLGRID");

			path_TB_EVENT_CELL = conf.get("mastercom.mroxdrmerge.locall.TB_EVENT_CELL");

			// 高铁场景
			path_TB_EVENT_AREA = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA");
			path_TB_EVENT_AREA_CELL = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA_CELL");
			path_TB_EVENT_AREA_CELLGRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA_CELLGRID");
			;
			path_TB_EVENT_AREA_GRID = conf.get("mastercom.mroxdrmerge.locall.path_TB_EVENT_AREA_GRID");
			;

			// outPathIndex: 1;stat1;/mt_wlyh/test1/$2;stat2;/mt_wlyh/test2/
			String outPathIndex = conf.get("mastercom.mroxdrmerge.locall.outpathindex");

			outpathIndexMap = new HashMap<Integer, String>();
			outtypeIndexMap = new HashMap<Integer, String>();
			for (String pathPare : outPathIndex.split("\\$"))
			{
				String[] ppPare = pathPare.split(";");

				String ppType = ppPare[1];
				ppType = StringHelper.SideTrim(ppType, " ");

				String ppPath = ppPare[2];
				if (ppPath.indexOf("hdfs://") >= 0)
				{
					int tm_sPos = ppPath.indexOf("/", ("hdfs://").length());
					ppPath = ppPath.substring(tm_sPos);
				}
				ppPath = StringHelper.SideTrim(ppPath, " ");
				ppPath = StringHelper.SideTrim(ppPath, "/");
				ppPath = StringHelper.SideTrim(ppPath, "\\\\");
				if (ppPath.indexOf("hdfs://") < 0)
				{
					ppPath = "/" + ppPath;
				}

				outpathIndexMap.put(Integer.parseInt(ppPare[0]), ppPath);
				outtypeIndexMap.put(Integer.parseInt(ppPare[0]), ppType);
			}
			// 初始化读取配置

			// 初始化输出控制
			if (outPathIndex.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			for (int index : outpathIndexMap.keySet())
			{
				mosMng.SetOutputPath(outtypeIndexMap.get(index), outpathIndexMap.get(index));
			}

			mosMng.SetOutputPath("tbEventMidInSample", path_TB_EVENT_MID_IN_SAMPLE);
			mosMng.SetOutputPath("tbEventMidOutSample", path_TB_EVENT_MID_OUT_SAMPLE);
			mosMng.SetOutputPath("tbEventLowInSample", path_TB_EVENT_LOW_IN_SAMPLE);
			mosMng.SetOutputPath("tbEventLowOutSample", path_TB_EVENT_LOW_OUT_SAMPLE);
			mosMng.SetOutputPath("tbEventHighInSample", path_TB_EVENT_HIGH_IN_SAMPLE);
			mosMng.SetOutputPath("tbEventHighOutSample", path_TB_EVENT_HIGH_OUT_SAMPLE);

			mosMng.SetOutputPath("tbEventHighOutGrid", path_TB_EVENT_HIGH_OUT_GRID);
			mosMng.SetOutputPath("tbEventMidOutGrid", path_TB_EVENT_MID_OUT_GRID);
			mosMng.SetOutputPath("tbEventLowOutGrid", path_TB_EVENT_LOW_OUT_GRID);

			mosMng.SetOutputPath("tbEventHighOutCellGrid", path_TB_EVENT_HIGH_OUT_CELLGRID);
			mosMng.SetOutputPath("tbEventMidOutCellGrid", path_TB_EVENT_MID_OUT_CELLGRID);
			mosMng.SetOutputPath("tbEventLowOutCellGrid", path_TB_EVENT_LOW_OUT_CELLGRID);

			mosMng.SetOutputPath("tbEventHighInGrid", path_TB_EVENT_HIGH_IN_GRID);
			mosMng.SetOutputPath("tbEventMidInGrid", path_TB_EVENT_MID_IN_GRID);
			mosMng.SetOutputPath("tbEventLowInGrid", path_TB_EVENT_LOW_IN_GRID);

			mosMng.SetOutputPath("tbEventHighInCellGrid", path_TB_EVENT_HIGH_IN_CELLGRID);
			mosMng.SetOutputPath("tbEventMidInCellGrid", path_TB_EVENT_MID_IN_CELLGRID);
			mosMng.SetOutputPath("tbEventLowInCellGrid", path_TB_EVENT_LOW_IN_CELLGRID);

			// build
			mosMng.SetOutputPath("tbEventHighBuildGrid", path_TB_EVENT_HIGH_BUILD_GRID);
			mosMng.SetOutputPath("tbEventMidBuildGrid", path_TB_EVENT_MID_BUILD_GRID);
			mosMng.SetOutputPath("tbEventLowBuildGrid", path_TB_EVENT_LOW_BUILD_GRID);

			mosMng.SetOutputPath("tbEventHighBuildCellGrid", path_TB_EVENT_HIGH_BUILD_CELLGRID);
			mosMng.SetOutputPath("tbEventMidBuildCellGrid", path_TB_EVENT_MID_BUILD_CELLGRID);
			mosMng.SetOutputPath("tbEventLowBuildCellGrid", path_TB_EVENT_LOW_BUILD_CELLGRID);

			mosMng.SetOutputPath("tbEventCell", path_TB_EVENT_CELL);

			// 高铁场景
			mosMng.SetOutputPath("tbArea", path_TB_EVENT_AREA);
			mosMng.SetOutputPath("tbAreaCell", path_TB_EVENT_AREA_CELL);
			mosMng.SetOutputPath("tbAreaGridCell", path_TB_EVENT_AREA_CELLGRID);
			mosMng.SetOutputPath("tbAreaGrid", path_TB_EVENT_AREA_GRID);

			mosMng.init();

			////////////////////
			dataStater = new DataStater(mosMng);

			StaticConfig.putCityNameByCityId();
			if (CellConfig.GetInstance().getLteCellInfoMap().size() == 0)
			{
				CellConfig.GetInstance().loadLteCell(conf);
			}
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			dataStater.outResult();
			super.cleanup(context);
			mosMng.close();
		}

		public void reduce(ImsiKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{

			if (key.getDataType() == XdrDataFactory.LOCTYPE_XDRLOC || key.getDataType() == XdrDataFactory.LOCTYPE_MRLOC)
			{
				if(key.getImsi()>0){
					// init location lib
					if (locItemMng != null && key.getImsi() != locItemMng.getImsi())
					{
						locItemMng = new LocItemMng();
						locItemMng.setImsi(key.getImsi());
					}

					if (locItemMng == null)
					{
						locItemMng = new LocItemMng();
						locItemMng.setImsi(key.getImsi());
					}

					for (Text tempLoc : values)
					{
						String[] vals = tempLoc.toString().split("\t", -1);
						LocItem item = LocItem.filleData(vals);
						locItemMng.addLocItem(item);
					}
				}
				else{
					if (locItemMng != null && 
							(key.getS1apid() != locItemMng.getS1apid() || key.getEci()!=locItemMng.getEci()))
							
					{
						locItemMng = new LocItemMng();
						locItemMng.setS1apid(key.getS1apid());
						locItemMng.setEci(key.getEci());
					}

					if (locItemMng == null)
					{
						locItemMng = new LocItemMng();
						locItemMng.setS1apid(key.getS1apid());
						locItemMng.setEci(key.getEci());
					}

					for (Text tempLoc : values)
					{
						String[] vals = tempLoc.toString().split("\t", -1);
						LocItem item = LocItem.filleData(vals);
						locItemMng.addLocItem(item);
					}
					
				}

			}
			else if (key.getDataType() > 100)
			{
				//
				if(key.getImsi()>0){
					if (locItemMng != null && key.getImsi() != locItemMng.getImsi())
					{
						locItemMng = null;
					}
				}else{
					if(locItemMng != null && 
							(key.getS1apid() != locItemMng.getS1apid() || key.getEci()!=locItemMng.getEci())){
						locItemMng = null;
					}
				}


				XdrDataBase tmpItem = null;
				try
				{
					tmpItem = XdrDataFactory.GetInstance().getXdrDataObject(key.getDataType());
				}
				catch (Exception e)
				{
					throw new InterruptedException("init data type error ");
				}

				curParseItem = tmpItem.getDataParseItem();
				if (curParseItem == null)
				{
					throw new IOException("parse item do not get.");
				}
				curDataAdapterReader = new DataAdapterReader(curParseItem);

				ArrayList<XdrDataBase> xdrDataBaseList = new ArrayList<XdrDataBase>();
				for (Text value : values)
				{
					XdrDataBase xdrDataItem = null;
					try
					{
						xdrDataItem = XdrDataFactory.GetInstance().getXdrDataObject(key.getDataType());
					}
					catch (Exception e)
					{
						throw new InterruptedException("init data type error ");
					}

					strs = value.toString().split(curParseItem.getSplitMark(), -1);
					curDataAdapterReader.readData(strs);
					try
					{
						if (!xdrDataItem.FillData(curDataAdapterReader))
						{
							LOGHelper.GetLogger().writeLog(LogType.error,
									"xdrdata fill data error :" + value.toString());
						}
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "xdrdata fill data error :" + value.toString());
					}

					xdrDataBaseList.add(xdrDataItem);
				}
				try
				{
					Collections.sort(xdrDataBaseList);
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "xdrDataBase sort error");
				}

				ArrayList<XdrDataBase> xdrDataLocList = new ArrayList<XdrDataBase>();
				// 给数据定位
				if (locItemMng != null)
				{
					xdrDataLocList = locItemMng.fillLoc(xdrDataBaseList);
				}

				if ("yes".equals(MainModel.GetInstance().getAppConfig().getOutXdrData()))
				{
					for (XdrDataBase xdrData : xdrDataBaseList)
					{
						// 输出源数据，加上定位信息
						outXdrData(key.getDataType(), xdrData);
					}
				}

				if (!"yes".equals(MainModel.GetInstance().getAppConfig().getToEventData()))
				{
					return;
				}
				
				/*****************************************************************************/
				if (key.getDataType() == XdrDataFactory.XDR_DATATYPE_HTTP)
				{
					//页面统计开关
					statHttpPage(xdrDataBaseList);
					
					// 加上视频卡顿的开关
					stateVidelLag(xdrDataLocList);
					
//					
				}
				/*******************************************************************************/

				/**
				 * TODO zhaikaishun 2017-10-23 待测试，xdrDataLocList变成xdrDataBaseList
				 */
				for (XdrDataBase xdrData : xdrDataBaseList)
				{
					// 数据运算
					ArrayList<EventData> eventDataList = xdrData.toEventData();

					if (eventDataList == null)
					{
						continue;
					}

					for (EventData eventData : eventDataList)
					{
						if (eventData.eventStat != null)
						{
							dataStater.stat(eventData);
						}

						if (eventData.eventDetial != null)
						{

							if (eventData.Interface == StaticConfig.INTERFACE_MOS_BEIJING
									|| eventData.Interface == StaticConfig.INTERFACE_WJTDH_BEIJING
									|| eventData.Interface == StaticConfig.INTERFACE_MME)
							{
								outEventData(eventData);
							}
							/**
							 * TODO 北京的新的接口加上http的数据,旧的不需要
							 */
							if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)
									&& eventData.Interface == StaticConfig.INTERFACE_S1U_HTTP){
								outEventData(eventData);
							}
							
							if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi)
									|| MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))
							{
								// 因为上面已经有输出了
								if(eventData.Interface != StaticConfig.INTERFACE_MME){
									outEventData(eventData);
								}
								
							}
							
							

						}
					}

				}

			}
		}

		private void stateVidelLag(ArrayList<XdrDataBase> xdrDataBaseList)
		{
			StatVideoLag statVideoLag = new StatVideoLag();
			
			ArrayList<EventData> eventDataListAll = statVideoLag.statVideoLag(xdrDataBaseList);
			if (eventDataListAll != null)
			{
				for (EventData eventData : eventDataListAll)
				{
					if (eventData.eventStat != null)
					{
						dataStater.stat(eventData);
					}

					if (eventData.eventDetial != null)
					{
						outEventData(eventData);
					}
				}
			}
		}

		public void statHttpPage(ArrayList<XdrDataBase> xdrDataBaseList)
		{
			HttpPageDeal.dataStater = dataStater;
			ArrayList<EventData> eventDataListAll = HttpPageDeal.deal(xdrDataBaseList);
			if (eventDataListAll != null)
			{
				for (EventData eventData : eventDataListAll)
				{
					if (eventData.eventStat != null)
					{
						dataStater.stat(eventData);
					}

					if (eventData.eventDetial != null)
					{
						outEventData(eventData);
					}
				}
			}
		}

		public void outXdrData(int dataType, XdrDataBase item)
		{
			tmsb.delete(0, tmsb.length());
			item.toString(tmsb);
			curText.set(tmsb.toString());

			try
			{
				if (tmsb.length() > 0)
				{
					mosMng.write(outtypeIndexMap.get(dataType), NullWritable.get(), curText);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "out xdr data error.");
			}

		}

		public void outEventData(EventData item)
		{
			tmsb.delete(0, tmsb.length());
			item.toString(tmsb);
			curText.set(tmsb.toString());

			try
			{
				if (item.confidentType == StaticConfig.OH)
				{
					mosMng.write("tbEventHighOutSample", NullWritable.get(), curText);

				}
				else if (item.confidentType == StaticConfig.OM)
				{
					mosMng.write("tbEventMidOutSample", NullWritable.get(), curText);
				}
				else if (item.confidentType == StaticConfig.OL)
				{

					mosMng.write("tbEventLowOutSample", NullWritable.get(), curText);

				}

				else if (item.confidentType == StaticConfig.IH)
				{

					mosMng.write("tbEventHighInSample", NullWritable.get(), curText);

				}
				else if (item.confidentType == StaticConfig.IM)
				{

					mosMng.write("tbEventMidInSample", NullWritable.get(), curText);

				}
				else if (item.confidentType == StaticConfig.IL)
				{

					mosMng.write("tbEventLowInSample", NullWritable.get(), curText);
				}
			}
			catch (Exception e)
			{
				// TODO handle exception
			}

		}

	}

}
