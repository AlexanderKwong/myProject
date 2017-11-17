package mro.lablefill_uemro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import StructData.SIGNAL_MR_All;
import StructData.StaticConfig;
import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;
import mro.lablefill.XdrLable;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;


public class XdrLableMng
{
	//基于imsi的xdr列表
	private Map<Long, ImsiS1apIDMngExtend> imsiS1apIDMap = new HashMap<Long, ImsiS1apIDMngExtend>();

	public XdrLableMng()
	{
		
	}

	public void addXdrLocItem(XdrLable xdrLable)
	{
		{
			ImsiS1apIDMngExtend mng = imsiS1apIDMap.get(xdrLable.imsi);
			if (mng == null)
			{
				mng = new ImsiS1apIDMngExtend();
				imsiS1apIDMap.put(xdrLable.imsi, mng);
			}
			mng.addXdrLocItem(xdrLable);
		}
	}

	public void init()
	{
		for (ImsiS1apIDMngExtend item : imsiS1apIDMap.values())
		{
			item.init();
		}
	}

	public void dealMroData(SIGNAL_MR_All mroItem)
	{
		
		{
	        //关联lable
			ImsiS1apIDMngExtend imsiMng = imsiS1apIDMap.get(mroItem.tsc.IMSI);
	        if(imsiMng == null)
	        {
	        	return;
	        }
	        
//	        LOGHelper.GetLogger().writeLog(LogType.error, "find the imsi:" + mroItem.tsc.IMSI + "");
	        		
			XdrLable lableItem = imsiMng.getXdrLoc(mroItem.tsc.beginTime);
	        if(lableItem != null)
	        {
	        	LOGHelper.GetLogger().writeLog(LogType.error, "find the lable:" + mroItem.tsc.IMSI + " " + mroItem.tsc.beginTime);
	        	
	            mroItem.testType = lableItem.testTypeGL;
	            mroItem.location = lableItem.locationGL;
	            mroItem.dist = lableItem.distGL;
	            mroItem.radius = lableItem.radiusGL;
	            mroItem.loctp = lableItem.loctpGL;
	            mroItem.indoor = lableItem.indoorGL;
	            mroItem.lable = lableItem.lableGL;
	            
	            mroItem.serviceType = lableItem.serviceType;
	            mroItem.subServiceType = lableItem.subServiceType;
	            
	            mroItem.moveDirect = lableItem.moveDirect;
	            
	            mroItem.tsc.UserLabel = lableItem.host;
	            mroItem.tsc.UserLabel = lableItem.wifiName;
	            
	            if(lableItem.testTypeGL == StaticConfig.TestType_DT || lableItem.testTypeGL == StaticConfig.TestType_DT_EX)
	            {
	            	//贵州数据要求60秒都可以回填
	        		if(MainModel.GetInstance().getCompile().Assert(CompileMark.GuiZhou)
	        		 || MainModel.GetInstance().getCompile().Assert(CompileMark.ShenZhen))
	        		{
		                if(Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 60)
		                {
		                	mroItem.tsc.longitude = lableItem.longitudeGL;
		                	mroItem.tsc.latitude = lableItem.latitudeGL;
		                }
	        		}
	        		else 
	        		{
		                if(Math.abs(mroItem.tsc.beginTime - lableItem.itime) <= 10)
		                {
		                	mroItem.tsc.longitude = lableItem.longitudeGL;
		                	mroItem.tsc.latitude = lableItem.latitudeGL;
		                }
	        		}
	           
	            }
	            else if(lableItem.testTypeGL == StaticConfig.TestType_CQT)
	            {
	            	mroItem.tsc.longitude = lableItem.longitudeGL;
	            	mroItem.tsc.latitude = lableItem.latitudeGL;
	            }

	        }
		
		}
	       
	}
	
	
	public class ImsiS1apIDMngExtend
	{
		private List<XdrLable> xdrLableList;
		private int timeExpend = 5*60;
		private int TimeSpan = 600;

		public ImsiS1apIDMngExtend()
		{
			xdrLableList = new ArrayList<XdrLable>();
		}

		public void addXdrLocItem(XdrLable lableItem)
		{
			xdrLableList.add(lableItem);
		}

		public void init()
		{
			Collections.sort(xdrLableList, new Comparator<XdrLable>()
			{
				public int compare(XdrLable a, XdrLable b)
				{
					return a.itime - b.itime;
				}
			});	
		}
		
		public XdrLable getXdrLoc(int tmTime)
		{
			int curDTTime = timeExpend;
			int curCQTTime = timeExpend;
			int curDTEXTime = timeExpend;
			XdrLable curDTItem = null; 
			XdrLable curCQTItem = null;
			XdrLable curDTEXItem = null;
			int tmTimeSpan = tmTime/TimeSpan*TimeSpan;
			int spTime;
				
			for(XdrLable item : xdrLableList)
			{
				if(item.itime/TimeSpan*TimeSpan != tmTimeSpan)
				{
					continue;
				}
				
				LOGHelper.GetLogger().writeLog(LogType.error, "get to same TimeSpan:" + item.testTypeGL + " " + item.longitudeGL);
				
				if(item.testTypeGL == StaticConfig.TestType_DT && item.longitudeGL > 0)
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "get to same TestType_DT " + item.longitudeGL + " " + item.loctimeGL );
					
					spTime = Math.abs(tmTime - item.loctimeGL );
					if(spTime < curDTTime)
					{
						curDTTime = spTime;
						curDTItem = item;
					}	
				}
				
				if(item.testTypeGL == StaticConfig.TestType_CQT && item.longitudeGL > 0)
				{
					spTime = Math.abs(tmTime - item.loctimeGL );
					if(spTime < curCQTTime)
					{
						curCQTTime = spTime;
						curCQTItem = item;
					}
				}
				
				if(item.testTypeGL == StaticConfig.TestType_DT_EX && item.longitudeGL > 0)
				{
					spTime = Math.abs(tmTime - item.loctimeGL );
					if(spTime < curDTEXTime)
					{
						curDTEXTime = spTime;
						curDTEXItem = item;
					}	
				}
			}
			
			if(curDTItem != null)
			{
				return curDTItem;
			}
			else if(curCQTItem != null)
			{
				return curCQTItem;
			}
			else if(curDTEXItem != null)
			{
				return curDTEXItem;
			}
			return null;
		}
		
	}
	
	
}
