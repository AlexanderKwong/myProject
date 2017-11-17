package jan.com.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import jan.util.IWriteLogCallBack;
import jan.util.LOGHelper;

public class DataDealReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> implements IWriteLogCallBack
{

	private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<KEYOUT, VALUEOUT> mos;
	protected Configuration conf;
	
	private String path_myLog;
	private int logLevel;
	
	protected final String utf8 = "UTF-8";
	protected Context context;
	protected String codetype;
	

	/**
	 * Called once at the beginning of the task.
	 */
	protected void setup(Context context) throws IOException, InterruptedException
	{	
		this.context = context;
		
		conf = context.getConfiguration();
		path_myLog = conf.get("mastercom.datamapper.path_myLog");
		logLevel = conf.get("mastercom.datamapper.myLoglevel") != null? Integer.parseInt(conf.get("mastercom.datamapper.myLoglevel")):LOGHelper.LogLevel_ALL;
		codetype = conf.get("mastercom.datamapper.codetype");
		
		mos = new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<KEYOUT, VALUEOUT>(context);
		
		LOGHelper.GetLogger().setLogLevel(logLevel);
		LOGHelper.GetLogger().addWriteLogCallBack(this);	
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		mos.close();
	}
	
	protected void setLogLevel(int logLevel)
	{
		this.logLevel = logLevel;
	}
	
	@Override
	public void writeLog(LogType logType, String strlog)
	{
		try
		{
			mos.write("myLogReduce", NullWritable.get(), LOGHelper.getFormatLog(logType, strlog), makeFileName(path_myLog, "myLogReduce"));
		}
		catch (Exception e)
		{
			//e.printStackTrace();
			// TODO: handle exception
		}
	}

	protected String makeFileName(String path, String name)
	{
		return path + "/" + name;
	}



}
