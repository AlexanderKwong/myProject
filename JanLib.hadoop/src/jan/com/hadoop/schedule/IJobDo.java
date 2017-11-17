package jan.com.hadoop.schedule;

import jan.util.IWriteLogCallBack;

public interface IJobDo
{
	public String getJobType();
	public int statJob(JobInfo jobInfo);
	public IWriteLogCallBack getLoger();
	
}
