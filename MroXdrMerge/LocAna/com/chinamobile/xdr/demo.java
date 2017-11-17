package com.chinamobile.xdr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.chinamobile.util.GisUtil;
import com.chinamobile.util.LocalFile;
import com.chinamobile.util.NotProguard;
import com.chinamobile.util.StringUtil;
import com.chinamobile.util.TimeUtil;

@NotProguard
public class demo
{
	@SuppressWarnings({
			"rawtypes", "unchecked"
	})
		
	public static List<LocationInfo> DecryptLoc(String requestType, String host, String url, String downlinkContent , String uplinkContent,boolean bHex)
	{
		List<LocationInfo> uplinkResult = new ArrayList<LocationInfo>();
		List<LocationInfo> downlinkResult = new ArrayList<LocationInfo>();
        LocationParseService lps = new LocationParseService();        
        
        if (url != null && host != null)
        {
        	lps.parseUriLocation(null, uplinkResult, requestType, host, url);
        }
		
        List<String>  wifiList = null;
        
        for(LocationInfo locInfo : uplinkResult)
        {
        	if(locInfo.wifiList!=null && locInfo.wifiList.size()>0)
        	{
        		wifiList = locInfo.wifiList;
        	}
        }
        	
        for(LocationInfo locInfo : downlinkResult)
        {
        	if(wifiList!=null && wifiList.size()>0
       		 && (locInfo.wifiList ==null || locInfo.wifiList.size()==0 ))
           	{
       			locInfo.wifiList = wifiList;
           	}
        }

		uplinkResult.addAll(downlinkResult);
		return uplinkResult;
	}


	public static int GetLocation(String host, String loctp)
	{
		if(loctp.toLowerCase().contains("get") || host == null)
			return 2;
		
		if(host.contains("baidu"))
			return 4;
		
		if(host.contains("amap.com"))
			return 5;
		
		if(host.contains("map.qq.com") || host.contains("tencent") )
			return 6;
		
		return 0;
	}
	
	public static String GetLoctp(String loctp)
	{
		if(loctp.toLowerCase().contains("get"))
			return "";
		
		if(loctp.contains("gps"))
			return "ll";
		else if(loctp.contains("net"))
			return "wf";
		else
		{
			return loctp;
		}
	}
	
	@SuppressWarnings("unused")
	public static void readDpiFileByLines(String fileName)
	{
		File file = new File(fileName);
		BufferedReader reader = null;
		BufferedWriter locInfoWriter = null;
		//try
		{
			try
			{
				reader = new BufferedReader(new FileReader(file));
				LocalFile.makeDir(outputRoot);
				locInfoWriter = new BufferedWriter(new FileWriter(outputRoot + "/" + file.getName() + ".dat"));
				String tempString = null;
				int line = 1;
				
				while ((tempString = reader.readLine()) != null)
				{
					String[] vct = tempString.split("\\|",-1);
					int nOffset = 0;
					if(vct[6].startsWith("460"))
						nOffset = 1;
					else if(vct[7].startsWith("460"))
						nOffset = 2;
					
					if(vct.length>=52 && vct[40].length()>0)
					{			
						//locInfoWriter.write(tempString + "\r\n");
						//System.out.println(
						//System.out.println(vct[32] +"," +vct[41] +"," + vct.length + "," +vct[vct.length-1]);
						String imsi	= vct[5+nOffset];
						String get_post_time	= vct[19+nOffset];	
						String response_time	= vct[20+nOffset];	
						//String event_type	= vct[3];	
						String ECI		= vct[16+nOffset];
						if(ECI.length()>5)
						{
							if(ECI_TYPE == 1)
							{
								long eci =  StringUtil.hexStringToLong(ECI.substring(0,ECI.length()-2))*256
										+ StringUtil.hexStringToLong(ECI.substring(ECI.length()-2));
								ECI = eci + "";
							}
						}
						String host		= vct[58+nOffset];
						String uri		= vct[59+nOffset];
						//String mme_sgw_ip	= vct[7];	
						//String enb_ip	= vct[8];	
						String source_ip	= vct[9+nOffset];
						String dest_ip	= vct[10+nOffset];
						String http_response_content = "";
						if(vct.length>77+nOffset)
							http_response_content= vct[77+nOffset];	
						
						String http_post_content	= "";
						 
						if(http_post_content == null)
							http_post_content = "";
						if(http_response_content == null)
							http_response_content = "";
						
						String requestType = "POST";
						List<LocationInfo> filledLocationInfoList = DecryptLoc(requestType,host,uri,
								http_response_content,http_post_content,false);
						if(filledLocationInfoList == null)
							continue;
						for (int i = 0; i < filledLocationInfoList.size(); i++)
					    {
					        try
					        {
					            //if(!filledLocationInfoList.get(i).requestType.contains("HTTP"))
					            {
					                LocationInfo finalInfo = filledLocationInfoList.get(i);

					                if(GisUtil.isValid(finalInfo.latitude,finalInfo.longitude))
					                {
					                    DecimalFormat df=new DecimalFormat(".#######");
					                    if(finalInfo.timeStamp ==0)
					                    	finalInfo.timeStamp = Long.parseLong(get_post_time);					                    
					                    if(locInfoWriter != null)
					                    {				                    	
					                    	locInfoWriter.write(
						                    	    imsi + "|"
						                    	+	get_post_time + "|"	
						                        +	finalInfo.timeStamp + "|"
						                        +   ECI + "|" //eci
						                        + "|"
						                        + "|" //port
						                    	+ "|"	
				                                + GetLocation(host,finalInfo.locationType) + "|"
				                                + GetLoctp(finalInfo.locationType)+"|"
						                    	+   finalInfo.radius+"|"
				                                +   df.format(finalInfo.longitude+GetjwSpan())+"|"
				                                +   df.format(finalInfo.latitude+GetjwSpan()) 
				                                + "|" + GetLocation(host, finalInfo.locationType)
				                                //+"|" + finalInfo.requestType
				                                //+"|" + finalInfo.host
				                                //+"|" + finalInfo.url
				                                + "\r\n");
					                    	
					                    	/*locInfoWriter.write(imsi+"|"
					                    		//+(new CalendarEx(finalInfo.frameTimeStamp)).toString(0)+"|"
				                                +(GetTimeSpan()+Long.parseLong(get_post_time))+"|"
					                    		+source_ip+"|"
				                                +dest_ip+"|"
				                                +GetLocation(finalInfo.host,finalInfo.locationType) + "|"
				                                +GetLoctp(finalInfo.locationType)+"|"
				                                +finalInfo.radius+"|"
				                                +df.format(finalInfo.longitude+GetjwSpan())+"|"
				                                +df.format(finalInfo.latitude+GetjwSpan())+"|"
				                                //+finalInfo.requestType+"|"
				                                +finalInfo.host+"|"
				                                +finalInfo.url
				                                +"\r\n");*/
					                    }
					                    
					                    //if(finalInfo.radius == 0 && finalInfo.locationType.contains("get"))
					                    //{
					                    //	//if(finalInfo.locationType.contains("gps") || finalInfo.radius<=20 /*&& finalInfo.radius<=100 && finalInfo.radius>0*/)
					                    //    {
					                    //        LatLng tmpBaiduLoc = GisUtil.GPS2Baidu(new LatLng(finalInfo.latitude, finalInfo.longitude));
					                    //       System.out.println("var point = new BMap.Point(" + tmpBaiduLoc.longitude + "," + tmpBaiduLoc.latitude + ");//" + finalInfo.frameIndex + "\r\naddMarker(point);");
					                    //	  }
					                    //}
					                }
					            }

					        } 
					        catch (Exception e)
					        {
					            e.printStackTrace();
					        }
					    }
					}				
				}
				locInfoWriter.close();
				reader.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}			
		}
	}
	
			
	@SuppressWarnings("unused")
	public static void readFileByLines(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists())
			return;
		BufferedReader reader = null;
		BufferedWriter locInfoWriter = null;
		BufferedWriter wifiInfoWriter = null;
		try
		{
			reader = new BufferedReader(new FileReader(file));
			locInfoWriter  = new BufferedWriter(new FileWriter(outputRoot + "/" + HdfsProcessDate + "/" + file.getName() + ".dat"));
            wifiInfoWriter = new BufferedWriter(new FileWriter(outputRoot + "/wifi/" + HdfsProcessDate + "/" + file.getName() + "."));
			
			String tempString = null;
			int line = 1;
			while ((tempString = reader.readLine()) != null)
			{
				//System.out.println("line " + line + ": " + tempString);
				/*imsi
				post_time
				event_type
				ci
				uri_main
				uri
				mme_sgw_ip
				?enb_ip
				source_ip?
				dest_ip
				http_response_content?
				http_post_content*/


				try
				{
					String[] vct = tempString.split("\t",-1);
					if(vct.length >= 12)
					{
						String imsi	= vct[0];
						String get_post_time	= vct[1].trim();	
						if(get_post_time.length()!=13)
							continue;
						String response_time	= vct[2];	
						String event_type	= vct[3];	
						String ECI		= vct[4];
						if(ECI.length()>=7)
						{
							try
							{
								int enbid = Integer.parseInt(ECI.substring(0,6));
								int ci = Integer.parseInt(ECI.substring(6,ECI.length()));
								ECI = (enbid *256 +ci) + "";
							}
							catch (Exception e)
							{
								ECI = "";
							}
						}
						String host		= vct[5];
						String uri		= vct[6];
						//System.out.println(uri_main);
						//String mme_sgw_ip	= vct[7];	
						//String enb_ip	= vct[8];	
						String source_ip	= vct[9];
						String dest_ip	= vct[10];
						String http_response_content = vct[11];						
						String http_post_content	= "";
						if(vct.length>=16)
						{
							if(vct[12].length()>0)
								http_post_content = vct[12];
							else if (vct[11].length()>0)
								http_post_content = vct[11];
						}
						 
						if(http_post_content == null)
							http_post_content = "";
						if(http_response_content == null)
							http_response_content = "";
						
						String requestType = event_type.trim().equals("16")?"POST":"GET";
						List<LocationInfo> filledLocationInfoList = DecryptLoc(requestType,host,uri,
								http_response_content,http_post_content,true);
						if(filledLocationInfoList == null)
							continue;
						for (int i = 0; i < filledLocationInfoList.size(); i++)
					    {  
					        try
					        {
				                LocationInfo finalInfo = filledLocationInfoList.get(i);
				                if(GisUtil.isValid(finalInfo.latitude,finalInfo.longitude))
				                {
				                    DecimalFormat df=new DecimalFormat(".#######");
				                   
				                    if(locInfoWriter != null 
				                    	&& (finalInfo.locationType.contains("gps") || finalInfo.radius<=20)
				                    )
				                    {	
				                    	if(finalInfo.timeStamp==0)
				                    		finalInfo.timeStamp = Long.parseLong(get_post_time);
				                    	
				                    	if(Math.abs(finalInfo.timeStamp-Long.parseLong(get_post_time))>600000)
				                    		continue;
				                    	
				                    	locInfoWriter.write(
				                    	    imsi + "|"
				                    	+	get_post_time + "|"	
				                        +	finalInfo.timeStamp + "|"
				                        +   ECI + "|" //eci
				                        +   "|"
				                        +   "|" //port
				                    	+   "|"	
				                    	+	3 + "|"		
				                    	+	"ll" + "|"	
		                                //+ GetLocation(host,finalInfo.locationType) + "|"
		                                //+ GetLoctp(finalInfo.locationType)+"|"
				                    	+   finalInfo.radius+"|"
		                                +   df.format(finalInfo.longitude+GetjwSpan())+"|"
		                                +   df.format(finalInfo.latitude+GetjwSpan()) 
		                                + "|" + GetLocation(host, finalInfo.locationType)
		                                //+ "|" + host + "|" + uri
		                                //+ "|" + finalInfo.coorType
		                                + "\r\n");				                    	
				                    }	
				                    
				                    if (finalInfo.wifiList != null && finalInfo.wifiList.size() > 0)
						            {
						                long wifiTime = finalInfo.timeStamp>0 ? finalInfo.timeStamp : finalInfo.frameTimeStamp;
						                
				                    	if(wifiTime==0)
				                    		wifiTime= Long.parseLong(get_post_time);
				                    	
				                    	String locCgiStr = null;
						                if(finalInfo.locCgi!=null)
						                {
						                    locCgiStr = finalInfo.locCgi.replaceAll("\\|",";");
						                }
		
						                wifiInfoWriter.write(imsi+"|"+wifiTime+"|"+finalInfo.longitude+"|"
						                        +finalInfo.latitude+"|"+ finalInfo.locationType + "|" +host+"|"+ uri+"|"+finalInfo.wifiList.toString()+"\r\n");
						            }
					            }		            
					        } 
					        catch (Exception e)
					        {
					            e.printStackTrace();
					        }
					    }
					}			
					line++;
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			reader.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (reader != null)
			{
				try
				{
					reader.close();
				}
				catch (IOException e1)
				{
				}
			}
			if(locInfoWriter != null)
	        {
	            try
				{
					locInfoWriter.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
	        }
			if(wifiInfoWriter != null)
	        {
	            try
				{
	            	wifiInfoWriter.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
	        }
		}
	}
	
	
	static boolean testPerf(String localDirname)
	{
		File dir = new File(localDirname);
		File[] files = dir.listFiles();
		if(files.length ==0)
			return false;

		//CalendarEx c1 = new CalendarEx(new Date());
		Date c1 = new Date(); 
		
		for(int i=0; i<files.length; i++)
		{
			readFileByLines(files[i].getAbsolutePath());
		}
		System.out.println(TimeUtil.getFormatTime(c1,"yyyy-MM-dd HH:mm:ss"));
		System.out.println(TimeUtil.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
		return true;
	}
	
	static String outputRoot = "d:/mastercom/temp";
	static String HdfsProcessDate = "";
	static int  ECI_TYPE = 0;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void readConfigInfo()
	{
		
	}
	    
    static long GetTimeSpan()
    {
    	return 0;
    	//long ret =(long)(Math.random() * 1000 - 500);
    	//return ret*4;
    }
    
    static double GetjwSpan()
    {
    	return 0;
    	//double ret = (Math.random()- 0.5)/100000.0;
    	//return ret*3;
    }
    
    static void ResultDeal(String fileName)
    {	
    	File file = new File(fileName);
		BufferedReader reader = null;
		BufferedWriter locInfoWriter = null;
		try
		{
			reader = new BufferedReader(new FileReader(file));
			locInfoWriter = new BufferedWriter(new FileWriter(outputRoot + "/" + file.getName() + ".dat"));
			String tempString = null;
			int line = 1;
			DecimalFormat df=new DecimalFormat(".#######");
			
			while ((tempString = reader.readLine()) != null)
			{
				
				try
				{
					String[] vct = tempString.split("\\|",-1);
					if(vct.length == 14)
					{
						//String imsi	= vct[0];
						String location	= vct[4];
						String loctp	= vct[5];
						String  reqType = vct[12];
						
						if(location.equals("2") && reqType.contains("tencent") )
						{
							location = "6";
							loctp = "wf";
						}
						else if(location.equals("2") && reqType.contains("amap") )
						{
							location = "5";
							loctp = "wf";
						}
						
						if(loctp.equals("net"))
						{
							loctp = "wf";
						}
							
                    	locInfoWriter.write(
                    	    vct[0] + "|"
                    	+	vct[1] + "|"	
                        +	vct[1] + "|"
                        +   0 + "|" //eci
                        +	vct[2] + "|"
                        +   0 + "|" //port
                    	+	vct[3] + "|"	
                    	+	location + "|"		
                    	+	loctp + "|"		
                    	+	vct[6] + "|"		
                    	+	vct[7] + "|"		
                    	+	vct[8] 		
                    	//+	vct[9] + "|"		
                     	//+	vct[10] 	
                        + "\r\n");
					}				
					line++;
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			reader.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (reader != null)
			{
				try
				{
					reader.close();
				}
				catch (IOException e1)
				{
				}
			}
			if(locInfoWriter != null)
	        {
	            try
				{
					locInfoWriter.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
	        }
		}
	}
}
