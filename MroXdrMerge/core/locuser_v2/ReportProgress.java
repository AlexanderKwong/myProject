package locuser_v2;

import jan.util.LOGHelper;
import jan.util.IWriteLogCallBack.LogType;

public class ReportProgress
{
	public void writeLog(int percentProgress, String userState)
	{
		if (percentProgress == 0)
		{
			LOGHelper.GetLogger().writeLog(LogType.info, userState);		
		}
		else if (percentProgress == -1)
		{
			LOGHelper.GetLogger().writeLog(LogType.debug, userState);
		}
	}
}
