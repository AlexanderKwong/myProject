package loc.imsifill;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import jan.com.hadoop.mapred.DataDealReducer;
import jan.com.hadoop.mapred.MultiOutputMng;
import jan.util.LOGHelper;
import mroxdrmerge.MainModel;
import util.DataGeter;

public class LocImsiFillReducer
{

	public static class StatReducer extends DataDealReducer<ImsiIPKey, Text, NullWritable, Text>
	{

		private MultiOutputMng<NullWritable, Text> mosMng;
		private Text curText = new Text();

		private String path_location;
		private String[] strs;
		private ImsiIPMng imsiIPMng;
		private StringBuffer sb = new StringBuffer();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			Configuration conf = context.getConfiguration();
			
			path_location = conf.get("mastercom.mroxdrmerge.loc.imsifill.path_location");
			// 初始化输出控制
			if (path_location.contains(":"))
				mosMng = new MultiOutputMng<NullWritable, Text>(context, "");
			else
				mosMng = new MultiOutputMng<NullWritable, Text>(context, MainModel.GetInstance().getFsUrl());
			mosMng.SetOutputPath("location", path_location);

			mosMng.init();
			////////////////////
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);

			mosMng.close();
		}

		public void reduce(ImsiIPKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			// imsi ip data
			if (key.getDataType() == 1)
			{
				imsiIPMng = new ImsiIPMng();

				for (Text value : values)
				{
					LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value);
		
					strs = value.toString().split("\t", -1);

					ImsiIPItem item = new ImsiIPItem();
					item.imsi = Long.parseLong(strs[1]);
					item.userIP = strs[2];
					item.userPort = Integer.parseInt(strs[3]);
					item.serverIP = strs[4];
					item.time = Integer.parseInt(strs[5]);
					try
					{
						item.eci = Integer.parseInt(strs[6]);
					}
					catch (NumberFormatException e)
					{
					}
					imsiIPMng.addImsiIPItem(item);
				}
				
				imsiIPMng.finInit();
			}
			// location data
			else if(key.getDataType() == 2)
			{
				long imsi = 0;
				int itime = 0;
				int httptime=0;
				int locTime = 0;
				int eci = 0;
				String userIP = "";
				int port = 0;
				String serverIP = "";
				int location = 0;
				String loctp = "";
				double radius = 0;
				double longitude = 0;
				double latitude = 0;
				
				long myImsi = 0;
				
				for (Text value : values)
				{
					//LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value);
					String company= "";
					String wifis="";
					String host = "";
					try
					{
						strs = value.toString().split("\\|", -1);
						
						itime = (int)(DataGeter.GetLong(strs[1])/1000L);
						locTime = (int)(DataGeter.GetLong(strs[2])/1000L);
						eci = DataGeter.GetInt(strs[3]);
						userIP = strs[4];
						port = DataGeter.GetInt(strs[5]);
						serverIP = strs[6];
						location = DataGeter.GetInt(strs[7]);
						loctp = strs[8];
						radius = DataGeter.GetDouble(strs[9]);
						longitude = DataGeter.GetDouble(strs[10]);
						latitude = DataGeter.GetDouble(strs[11]);
						if(strs.length>=15)
						{
							company = strs[12];
							wifis = strs[13];
							host  = strs[14];
						}
						
						if(strs[0].indexOf('\t')>0)
						{
							strs[0] = strs[0].substring(strs[0].indexOf('\t')+1);
						}
						try
						{
							imsi = DataGeter.GetLong(strs[0]);
						}
						catch (Exception e)
						{}
						
						myImsi = 0;
						ImsiIPItem imsiIPItem = null;	
						
	                    if(imsiIPMng != null)
	                    {
	                    	imsiIPItem = imsiIPMng.getNearestImsiIP(itime);
	                    	if(imsiIPItem != null)
	                    	{
	                    		myImsi = imsiIPItem.imsi;
	                    		httptime = imsiIPItem.time;
	                    		eci = imsiIPItem.eci;
	                    		if(itime==locTime)
	                    		{//说明locTime等于采集时间，则采集时间要改成http XDR的时间。
	                    			locTime = httptime;
	                    		}
	                    	}
	                    }
	                    
	                    if(imsi <= 0)
	                    {
	                    	imsi = myImsi;
	                    }
					}
					catch (Exception e)
					{
						LOGHelper.GetLogger().writeLog(LogType.error, "", e);
					}
					
					if(imsi>0)
					{			         
	                    sb.delete(0, sb.length());
	                    sb.append(imsi);sb.append("|");
	                    sb.append(httptime*1000L);sb.append("|");
	                    sb.append(locTime*1000L);sb.append("|");
	                    sb.append(eci);sb.append("|");
	                    sb.append(userIP);sb.append("|");
	                    sb.append(port);sb.append("|");
	                    sb.append(serverIP);sb.append("|");
	                    sb.append(location);sb.append("|");
	                    sb.append(loctp);sb.append("|");
	                    sb.append(radius);sb.append("|");
	                    sb.append(longitude);sb.append("|");
	                    sb.append(latitude);
	                    sb.append("|");sb.append(company);
	                    sb.append("|");sb.append(wifis);
	                    sb.append("|");sb.append(host);
	                    sb.append("|");sb.append(myImsi);                    
	                    curText.set(sb.toString());
	                    mosMng.write("location", NullWritable.get(), curText);
						LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value + "|ok");
					}
					else
					{
						LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value + "|fail");
					}
				}				
				imsiIPMng = null;
			}	
		}
	}
}
