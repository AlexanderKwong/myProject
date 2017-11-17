import java.text.SimpleDateFormat;
import java.util.Date;

import jan.util.IWriteLogCallBack;
import jan.util.SSHHelper;
import mroxdrmerge.MainModel;

public class XdrMroMerge implements IWriteLogCallBack
{
	private Date statDate = null;
   
	public XdrMroMerge(Date statDate)
	{
	   this.statDate = statDate;	
	}
	
	public boolean run()
	{
		try
		{		
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			String cmd = String.format("hadoop jar %s/MroXdrMerge/MroXdrMerge_allstat.jar %s %s %s", MainModel.GetInstance().getExePath(), "network", "01_" + format.format(statDate).substring(2, 8), "/result/" + format.format(statDate));
			
	        //执行脚本
			SSHHelper sshHelper = new SSHHelper(MainModel.GetInstance().getAppConfig().getSSHHost(),
					MainModel.GetInstance().getAppConfig().getSSHPort(),
					MainModel.GetInstance().getAppConfig().getSSHUser(),
					MainModel.GetInstance().getAppConfig().getSSHPwd(), 
					this);
			
			System.out.println("开始执行Mro&Xdr关联...");
			sshHelper.excuteCmd(cmd, 2);
			System.out.println("执行Mro&Xdr关联！");
			
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public void writeLog(LogType type, String strlog)
	{
		System.out.println(strlog);
	}
	
	
	
	
	
}
