package jan.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SSHHelper
{
	private JSch jsch = new JSch();
	private Session session;
	private IWriteLogCallBack writeLog;

	public SSHHelper(String host, int port, String user, String pwd, IWriteLogCallBack writeLog) throws JSchException
	{
		session = jsch.getSession(user, host, port);
		session.setPassword(pwd);
		session.setTimeout(0);
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);

		this.writeLog = writeLog;
	}

	
	//resultType: 1 getInputStream; 2 getExtInputStream
	public boolean excuteCmd(String cmd, int resultType)
	{
		return excuteCmd(cmd,resultType,null);
	}
	
	//resultType: 1 getInputStream; 2 getExtInputStream
	public boolean excuteCmd(String cmd, int resultType, IErrorFileDealCallBack errorFileCallBack)
	{
		Channel channel = null;
		
		try
		{
			cmd = "source /etc/profile;source ~/.bash_profile;" + cmd;
			session.connect();
			channel = session.openChannel("exec");
			ChannelExec execChannel = (ChannelExec) channel;
			execChannel.setCommand(cmd);
			
			channel.setInputStream(null);
			
			BufferedReader input = null;
			if(resultType == 1)
			{
			    input = new BufferedReader(new InputStreamReader(channel.getInputStream(), "UTF-8"));
			}
			else if(resultType == 2)
			{
				input = new BufferedReader(new InputStreamReader(channel.getExtInputStream(), "UTF-8"));
			}
			else 
			{
				return false;
			}
			
			channel.connect();
			if (writeLog != null)
			{
				writeLog.writeLog(IWriteLogCallBack.LogType.info, "Connect Channel Success.");
			}
			
			String line = "";
			while ((line = input.readLine()) != null)
			{
				if(errorFileCallBack!=null &&
						line.contains("BlockMissingException") 
						&& line.contains("file="))
				{
					errorFileCallBack.DealErrorFile(line.substring(line.indexOf("file=")+5));
					//System.out.println(line.substring(line.indexOf("file=")+5));
				}
				if (writeLog != null)
				{
					writeLog.writeLog(IWriteLogCallBack.LogType.info, line);
				}
			}
			input.close();
			
			Thread.sleep(1000); 
		}
		catch (Exception e)
		{
			if (writeLog != null)
			{
				writeLog.writeLog(IWriteLogCallBack.LogType.error, e.getMessage());
			}
			return false;
		}
		finally
		{
			channel.disconnect();
			session.disconnect();
		}
		if (writeLog != null)
		{
			writeLog.writeLog(IWriteLogCallBack.LogType.info, "Connect Channel Success.");
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception 
	{ 			
		SSHHelper ssh = new SSHHelper("192.168.1.31",22,"hmaster","mastercom168",null);
		ssh.excuteCmd("date", 1);
		System.out.println("ok");
	}
 
}
