package jan.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import jan.util.IWriteLogCallBack.LogType;

public class LOGHelper
{
	public static final int LogLevel_ALL = 100;
	public static final int LogLevel_DEBUG = 40;
	public static final int LogLevel_INFO = 30;
	public static final int LogLevel_WARN = 20;
	public static final int LogLevel_ERROR = 10;
	public static final int LogLevel_NONE = 0;
	
	
	private List<IWriteLogCallBack> logList = new ArrayList<IWriteLogCallBack>();
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(); 
	private PrintStream printStream = new PrintStream(byteArrayOutputStream);
	private int logLevel = LogLevel_ALL;
	
	private LOGHelper()
	{
		
	}
	
	private static LOGHelper instance;
	public static LOGHelper GetLogger()
	{
		if(instance == null)
		{
			instance = new LOGHelper();
		}
		return instance;
	}
	
	public void addWriteLogCallBack(IWriteLogCallBack writeLogCallBack)
	{
		logList.add(writeLogCallBack);
	}
	
	public void removeWriteLogCallBack(IWriteLogCallBack writeLogCallBack)
	{
		logList.remove(writeLogCallBack);
	}
	
	public void writeLog(LogType logType, String log, Exception e)
	{
		byteArrayOutputStream.reset();
		e.printStackTrace(printStream);  	
		String strlog = log + "\n" + "MESSAGE：" + e.getMessage() + "\n" + byteArrayOutputStream.toString();
		writeLog(logType, strlog);
	}
	
	public void writeLog(LogType logType, String strlog)
	{
		if(logType == LogType.info)
		{
			if(logLevel >= LogLevel_INFO)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}
			}
		}
		else if(logType == LogType.debug)
		{
			if(logLevel >= LogLevel_DEBUG)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}
			}
		}
		else if(logType == LogType.warn)
		{
			if(logLevel >= LogLevel_WARN)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}
			}
		}
		else if(logType == LogType.error)
		{
			if(logLevel >= LogLevel_ERROR)
			{
				for(IWriteLogCallBack item : logList)
				{
					item.writeLog(logType, strlog);
				}	
			}			
		}
		
		return;	
	}
	
	public static String getFormatLog(LogType logType, String log)
	{
		if(logType == LogType.info)
		{
			return getFormatLogInfo(log);
		}
		else if(logType == LogType.debug)
		{
			return getFormatLogDebug(log);
		}
		else if(logType == LogType.warn)
		{
			return getFormatLogWarn(log);
		}
		else if(logType == LogType.error)
		{
			return getFormatLogError(log);
		}
        return log;
	}	
	
	public int getLogLevel()
	{
		return logLevel;
	}
	
	public void setLogLevel(int logLevel)
	{
		this.logLevel = logLevel;
	}

    public static String getFormatLogInfo(String log)
    {
    	return "[" + format.format(new Date()) + "][INFO] " + log;
    }
    
    public static String getFormatLogWarn(String log)
    {
    	return "[" + format.format(new Date()) + "][WARN] " + log;
    }
    
    public static String getFormatLogError(String log)
    {
    	return "[" + format.format(new Date()) + "][ERROR] " + log;
    }
    
    public static String getFormatLogDebug(String log)
    {
    	return "[" + format.format(new Date()) + "][DEBUG] " + log;
    }
	
}
