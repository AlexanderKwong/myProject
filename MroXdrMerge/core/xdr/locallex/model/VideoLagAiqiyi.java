package xdr.locallex.model;

import StructData.StaticConfig;
import mroxdrmerge.CompileMark;
import mroxdrmerge.MainModel;
import xdr.locallex.EventData;
import xdr.locallex.EventDataStruct;

/**
 * 
 * @author ZhaiKaiShun 2017-10-21
 *
 */
public class VideoLagAiqiyi
{
	//基础数据  
	public int App_Typ;
	public String Refer_URI;
	public int longitude;
	public int latitude;
	public int iCityID; 
	public int istime;
	public String strloctp;
	public String label;
	public int ibuildid;
	public int ibuildheight;
	public long Eci;
	public int Interface;
	public int testType;
	public int iDoorType;
	public int locSource;
	public int confidentType;
	public int iAreaType;
	public int iAreaID;
	
	
	public String HOST;
	public String URI;
	public String cityID;
	public int ECI;
	public long Procedure_Start_Time;
	public long imsi;
    	
	public long  ct_adstart;  //广告开播
	public long  starttm;  //视频开播时出现
	public long ct_addend; //广告结束
	public long core_t_8; // 视频卡顿特征
	public long isfinish_1_ishcdn; //isfinish=1&ishcdn视频正常播放结束特征
	public long isfinish_2_ishcdn; //isfinish=2&ishcdn 视频中途退出特征  
	public long core_t_1_starttm ; //core?t=1%starttm= 视频开播特征
	 
	public long 首播时延累加;
	public int 卡顿次数;
	public int starttmNums;
	public int isfinishNums;
	private int 播放次数;
	public int 播放失败次数;
	 
	public void fillOriginData(XdrDataBase xdrDataBase){
		// 1. 基本经纬度的添加 添加过一次就算了
		XdrData_Http xdrData_Http = (XdrData_Http)xdrDataBase;
		
		Procedure_Start_Time = xdrData_Http.istime*1000L+xdrData_Http.istimems;

		App_Typ = xdrData_Http.App_Type;
		
		HOST = xdrData_Http.HOST;
		URI = xdrData_Http.URI; 
		Refer_URI = xdrData_Http.Refer_URI; 
		longitude = xdrData_Http.iLongitude;
		latitude = xdrData_Http.iLatitude;
		
		//其他的几个
		iCityID=xdrData_Http.iCityID;
		istime = xdrData_Http.istime;
//		wtimems = xdrData_Http.wTimems;
		strloctp = "";
		label = xdrData_Http.label;
		ibuildid = xdrData_Http.ibuildid;
		
		ibuildheight =xdrData_Http.iheight;
		
		imsi = xdrData_Http.imsi;
		Eci = (int) xdrData_Http.Eci;
		
		Interface = StaticConfig.INTERFACE_S1_U;
			
		testType = xdrData_Http.testType;
		iDoorType = xdrData_Http.iDoorType;
		locSource = xdrData_Http.locSource;
		
		confidentType = xdrData_Http.confidentType;
		iAreaType = xdrData_Http.iAreaType;
		iAreaID = xdrData_Http.iAreaID;
		
	}
	
	
	public void fillData(XdrDataBase xdrDataBase){
		
		XdrData_Http xdrData_Http = (XdrData_Http)xdrDataBase;
		URI = xdrData_Http.URI;
		
		if(confidentType<=0){
			fillOriginData(xdrDataBase);
		}
		
		
		//首播时延 和 
		long delayTime=0;
		if(URI.contains("core?t=1") && URI.contains("starttm=")){ 
			播放次数++;
			String delayStr = URI.split("starttm=", 2)[1].split("&", 2)[0];
			try{
				delayTime = Long.parseLong(delayStr);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		首播时延累加 = 首播时延累加 + delayTime;
		
		//卡顿次数  
		if(URI.contains("core?t=8&")){
			卡顿次数++;
		}
		//starttm次数
		if(URI.contains("core?t=1&")){
			starttmNums++;
		}
		
		//isfinish次数 //TODO
		if(URI.contains("isfinish=")){
			isfinishNums++;
		}
		//播放失败数  
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing)){
			if(URI.contains("msg.71.am/core?t=0")){  //TODO 失败次数可能不是这样的
				播放失败次数++;
			}
			
		}else{
			if(URI.contains("msg.71.am/core?t=0")){ 
				播放失败次数++;
			}	
		}

		
	}
	
	public EventData toEvent()
	{
		EventData eventData = new EventData();
		eventData.eventStat = new EventDataStruct();
		eventData.iCityID=iCityID; 
		eventData.iTime=istime;
		eventData.wTimems = 0; //istimems;//没用，给0
		eventData.strLoctp = strloctp;
		eventData.strLabel = label;
		eventData.iLongitude = longitude;
		eventData.iLatitude = latitude;
		eventData.iBuildID=ibuildid;
		eventData.iHeight = ibuildheight;
		eventData.IMSI = imsi;
		eventData.iEci = (int)Eci;
		eventData.Interface = StaticConfig.INTERFACE_S1_U;
		eventData.iKpiSet = 2;
		eventData.iProcedureType = 1;

		eventData.iTestType = testType;
		eventData.iDoorType = iDoorType;
		eventData.iLocSource = locSource;
		eventData.confidentType = confidentType;
		eventData.iAreaType = iAreaType;
		eventData.iAreaID = iAreaID;
		
		//原始的数据加载
		
		//本地的这些数据加载  
		eventData.eventStat.fvalue[0] = 首播时延累加;
		eventData.eventStat.fvalue[1] = 卡顿次数;
		eventData.eventStat.fvalue[2] = 0;    //卡顿总时长, 因为无法统计;
		eventData.eventStat.fvalue[3] = 0;   //播放时长, 爱奇艺的无法统计	
		eventData.eventStat.fvalue[4] = 播放次数;;  //播放次数
		eventData.eventStat.fvalue[5] = 播放失败次数;
		return eventData;
	}
	

}
