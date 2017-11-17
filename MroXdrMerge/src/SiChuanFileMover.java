
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import jan.com.hadoop.hdfs.HDFSOper;
import mroxdrmerge.MainModel;

public class SiChuanFileMover
{
	protected static Logger LOG = Logger.getLogger(SiChuanFileMover.class);

	public static void main(String args[])
	{
		doSiChuanMergestat(args[0], args[1]);
	}

	public static void doSiChuanMergestat(String dataTime, String queueName)
	{
		Configuration conf = new Configuration();
		String mroXdrMergePath = MainModel.GetInstance().getAppConfig().getMroXdrMergePath();
		try
		{
			HDFSOper hdfsOper = new HDFSOper(conf);
			String day = dataTime.replace("01_", "");
			String dayHour = "";

			String xdrLocPath = "/wangyou/mastercom/mroxdrmerge/xdr_loc/data_" + dataTime;
			String mroLocPath = "/wangyou/mastercom/mroxdrmerge/mro_loc/data_" + dataTime;
			String xdrsrcPath = "";
			String mrosrcPath = "";
			String suf = "";
			long start = System.currentTimeMillis();
			for (int i = 0; i < 24; i++)
			{
				if (i < 10)
				{
					suf = "0" + i;
				}
				else
				{
					suf = "" + i;
				}
				xdrsrcPath = xdrLocPath + suf;
				mrosrcPath = mroLocPath + suf;
				dayHour = day + suf;
				// 移动xdr数据
				LOG.info("begin mover ");
				moveFile(hdfsOper, xdrsrcPath, "Mins,MYLOG,output", dayHour, day, suf);
				moveFile(hdfsOper, mrosrcPath, "Mins,MYLOG,output", dayHour, day, suf);
			}
			System.out.println("move file finished ！ Cost: " + (System.currentTimeMillis()-start) + "ms");
			System.out.println("begin merge ！");

//			MergestatGroup.doMergestatGroup(queueName, dataTime, mroXdrMergePath, conf, hdfsOper);
			System.out.println("merge finished !");
		}
		catch (Exception e)
		{
			LOG.info("mover error !", e);
		}
	}

	/**
	 * 
	 * @param hdfsOper
	 * @param srcFolder
	 *            原始文件夹
	 * @param filterWords
	 *            过滤
	 * @param replaceFolder
	 *            要替换的文件夹
	 * @param suf
	 *            文件名添加后缀
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static void moveFile(HDFSOper hdfsOper, String srcFolder, String filterWords, String dayhour, String day, String suf) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		if (hdfsOper.checkDirExist(srcFolder))
		{
			LOG.info("begin moving " + srcFolder);
			List<FileStatus> fileList = hdfsOper.listFlolderStatus(srcFolder);
			for (FileStatus fs : fileList)
			{
				String path = fs.getPath().toString();
				if (filter(path, filterWords))
				{
					LOG.info(" moving " + path);
					List<FileStatus> tempList = hdfsOper.listFileStatus(path, true);
					LOG.info("filesize:" + tempList.size());
					for (FileStatus tmpfs : tempList)
					{
						String temppath = tmpfs.getPath().toString();
						if (filter(temppath, filterWords))
						{
							Path dPath = new Path(temppath.replace(dayhour, day) + "_" + suf);
							if (!hdfsOper.checkDirExist(dPath.getParent().toString()))
							{
								hdfsOper.mkdir(dPath.getParent().toString());
							}
							if (hdfsOper.getHdfs().rename(tmpfs.getPath(), dPath))
							{
								System.out.println("moving  " + temppath + "  to " + (temppath.replace(dayhour, day) + "_" + suf) + " successfull !");
							}
							else
							{
								System.out.println("moving" + temppath + "  to " + (temppath.replace(dayhour, day) + "_" + suf) + " fail！");
							}
						}
					}
					tempList.clear();
				}
			}
			fileList.clear();
		}
	}

	public static boolean filter(String path, String filterWord)
	{
		String[] filters = filterWord.split(",", -1);
		for (String filter : filters)
		{
			if (path.contains(filter))
			{
				return false;
			}
		}
		return true;
	}

}
