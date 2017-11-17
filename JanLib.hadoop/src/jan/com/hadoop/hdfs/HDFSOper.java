package jan.com.hadoop.hdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;

public class HDFSOper
{
	public static final String FS_DEFAULT_NAME_KEY = "fs.defaultFS";

	private Logger log = Logger.getLogger(HDFSOper.class.getName());

	private Configuration conf = new Configuration();
	private FileSystem fs;
	private DistributedFileSystem hdfs;
	private String hadoopHost;
	private int hadoopPort;
	// zks
	public FileStatus fileStatus;

	public HDFSOper(String hdfsUrl) throws Exception
	{
		if (hdfsUrl.length() == 0)
		{
			hdfsUrl = conf.get(FS_DEFAULT_NAME_KEY);
		}

		{
			String tmStr = hdfsUrl.replace("hdfs://", "");
			if (tmStr.indexOf(":") > 0)
			{
				this.hadoopHost = tmStr.substring(0, tmStr.indexOf(":"));
				this.hadoopPort = Integer.parseInt(tmStr.substring(tmStr.indexOf(":") + 1));
			}
			else
			{
				this.hadoopHost = tmStr;
				this.hadoopPort = 9000;
			}
		}

		FileSystem.setDefaultUri(conf, hdfsUrl);
		fs = FileSystem.get(conf);
		// fileStatus =fs.getFileStatus(new
		// Path("hdfs://192.168.1.31:9000/filtertest/input"));
		hdfs = (DistributedFileSystem) fs;
	}

	public FileStatus getFileLength(String path) throws Exception
	{
		fileStatus = fs.getFileStatus(new Path(path));
		return fileStatus;
	}

	public HDFSOper(String hadoopHost, int port) throws Exception
	{
		this(hadoopHost.trim().length() > 0 ? "hdfs://" + hadoopHost + ":" + port : "");
	}

	private void SetHadoopRoot(String HADOOP_URL)
	{
		try
		{
			if (HADOOP_URL.length() < 6)
				return;
			System.out.println("SetHadoopRoot： " + HADOOP_URL);
			FileSystem.setDefaultUri(conf, HADOOP_URL);
			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		}
		catch (Exception e)
		{
			System.out.println("SetHadoopRoot error： " + e.getMessage());
			e.printStackTrace();
		}
	}

	private void SetHadoopRoot()
	{
		SetHadoopRoot(getUrl());
	}

	public HDFSOper(String[] hadoopHost, int port) throws Exception
	{
		for (int i = 0; i < hadoopHost.length; i++)
		{
			SetHadoopRoot("hdfs://" + hadoopHost[i] + ":" + port);
			if (!checkStandbyException("/"))
				break;
		}
	}

	public String getUrl()
	{
		return conf.get(FS_DEFAULT_NAME_KEY);
	}

	public String getUrl(String hadoopHost, int port)
	{
		if (hadoopHost.trim().length() == 0)
		{
			return conf.get(FS_DEFAULT_NAME_KEY);
		}
		return "hdfs://" + hadoopHost + ":" + port;
	}

	public HDFSOper(Configuration conf) throws Exception
	{
		try
		{
			this.conf = conf;

			fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;

			String hdfsUrl = conf.get(FS_DEFAULT_NAME_KEY);
			String tmStr = hdfsUrl.replace("hdfs://", "");
			if (tmStr.indexOf(":") > 0)
			{
				this.hadoopHost = tmStr.substring(0, tmStr.indexOf(":"));
				this.hadoopPort = Integer.parseInt(tmStr.substring(tmStr.indexOf(":") + 1));
			}
			else
			{
				this.hadoopHost = tmStr;
				this.hadoopPort = 8020;
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw e;
		}
	}

	public boolean closeAll()
	{
		try
		{
			FileSystem.closeAll();
			SetHadoopRoot();
			return checkFileExist("/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public DistributedFileSystem getHdfs()
	{
		return hdfs;
	}

	public FileSystem getFs()
	{
		return fs;
	}

	public String getHadoopHost()
	{
		return hadoopHost;
	}

	public int getHadoopPort()
	{
		return hadoopPort;
	}

	public void moveSmallFilesToParent(String parentDir)
	{
		System.out.println("MoveSmallFilesToParent： " + parentDir);
		FileStatus fileStatus[];
		try
		{
			fileStatus = fs.listStatus(new Path(parentDir));
			int listlength = fileStatus.length;

			for (int i = 0; i < listlength; i++)
			{
				if (fileStatus[i].isDirectory() == true)
				{
					String pathName = fileStatus[i].getPath().getName().toLowerCase();
					/*
					 * if (!pathName.contains("sample") &&
					 * !pathName.contains("event") &&
					 * !pathName.contains("grid")) { continue; }
					 */

					FileStatus childStatus[] = fs.listStatus(new Path(parentDir + "/" + pathName));

					int childListlength = childStatus.length;
					boolean bShouldMove = false;
					try
					{
						for (int j = 0; j < childListlength; j++)
						{
							if (childStatus[j].isDirectory() == false)
							{
								String childName = childStatus[j].getPath().getName().toLowerCase();
								if (!childName.contains("sample") && !childName.contains("event") && !childName.contains("grid"))
								{
									continue;
								}
								movefile(parentDir + "/" + pathName + "/" + childName, parentDir);
								bShouldMove = true;
							}
						}
						if (bShouldMove)
						{
							fs.delete(new Path(parentDir + "/" + pathName));
							System.out.println("删除文件夹成功: " + parentDir + "/" + pathName);
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 列出所有DataNode的名字信息
	 */
	public void listDataNodeInfo()
	{
		try
		{
			DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodeStats.length];
			System.out.println("List of all the datanode in the HDFS cluster:");

			for (int i = 0; i < names.length; i++)
			{
				names[i] = dataNodeStats[i].getHostName();
				System.out.println(names[i]);
			}
			System.out.println(hdfs.getUri().toString());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 检测是否是备用节点
	 * 
	 * @throws Exception
	 */
	public boolean checkStandbyException(String filename)
	{
		try
		{
			Path f = new Path(filename);
			hdfs.exists(f);
		}
		catch (org.apache.hadoop.ipc.RemoteException e)
		{
			if (e.getClassName().equals("org.apache.hadoop.ipc.StandbyException"))
			{
				return true;
			}
		}
		catch (Exception e)
		{

		}
		return false;
	}

	/**
	 * 得到文件夹下文件个数
	 * 
	 * @param srcPath
	 * @return
	 */
	public int getFileCount(String srcPath)
	{
		int num = 0;
		try
		{
			for (String path : srcPath.split(",", -1))
			{
				if (checkFileExist(path))
				{
					Path p1 = new Path(path);
					FileStatus[] fst = hdfs.listStatus(p1);
					num += fst.length;
				}
			}
		}
		catch (Exception e)
		{
			return 0;
		}
		return num;
	}

	public void movefile(String src, String dst) throws Exception
	{
		Path p1 = new Path(src);
		Path p2 = new Path(dst);
		hdfs.rename(p1, p2);
		System.out.println("重命名文件夹或文件成功: " + src + " --> " + dst);
	}

	public boolean delete(String src) throws Exception
	{
		try
		{
			Path p1 = new Path(src);
			if (hdfs.isDirectory(p1))
			{
				hdfs.delete(p1, true);
				System.out.println("删除文件夹成功: " + src);
			}
			else if (hdfs.isFile(p1))
			{
				hdfs.delete(p1, false);
				System.out.println("删除文件成功: " + src);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public void reNameExistsPath(String srcPath) throws Exception
	{
		String tarPath;
		// 检测输出目录是否存在，存在就改名
		if (checkFileExist(srcPath))
		{
			tarPath = srcPath.trim();
			while (tarPath.charAt(tarPath.length() - 1) == '/')
			{
				tarPath = tarPath.substring(0, tarPath.length() - 1);
			}
			Date now = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmss");
			String nowStr = dateFormat.format(now);
			tarPath += "_" + nowStr;
			movefile(srcPath, tarPath);
		}
		else
		{
			tarPath = srcPath;
		}
	}

	/**
	 * 查看文件是否存在
	 */
	public boolean checkFileExist(String filename)
	{
		try
		{
			Path a = hdfs.getHomeDirectory();
			// System.out.println("main path:" + a.toString());
			Path f = new Path(filename);
			return hdfs.exists(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 查看路径是否存在
	 */
	public boolean checkDirExist(String dirPath)
	{
		try
		{
			Path a = hdfs.getHomeDirectory();
			// System.out.println("main path:" + a.toString());
			Path f = new Path(dirPath);
			return hdfs.exists(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 创建文件到HDFS系统上
	 */
	public boolean mkfile(String filePath)
	{
		try
		{
			Path f = new Path(filePath);
			FSDataOutputStream os = fs.create(f, true);
			Writer out = new OutputStreamWriter(os, "utf-8");// 以UTF-8格式写入文件，不乱码
			out.close();
			os.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return true;
		}
		return false;
	}

	public boolean mkdir(String dirName)
	{
		if (checkFileExist(dirName))
			return true;
		try
		{
			Path f = new Path(dirName);
			System.out.println("Create and Write :" + f.getName() + " to hdfs");
			return hdfs.mkdirs(f);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return false;
	}

	/**
	 * 读取本地文件到HDFS系统<br>
	 * 请保证文件格式一直是UTF-8，从本地->HDFS
	 */
	public boolean copyFileToHDFS(String localFilename, String hdfsPath)
	{
		try
		{
			System.out.println("Begin move " + localFilename + " to " + hdfsPath);
			mkdir(hdfsPath);

			File file = new File(localFilename);
			FileInputStream is = new FileInputStream(file);

			if (this.checkFileExist(hdfsPath + "/" + file.getName()))
			{
				delete(hdfsPath + "/" + file.getName());
			}

			Path f = new Path(hdfsPath + "/" + file.getName());

			FSDataOutputStream os = fs.create(f, true);
			byte[] buffer = new byte[10240000];
			int nCount = 0;

			while (true)
			{
				int bytesRead = is.read(buffer);
				if (bytesRead <= 0)
				{
					break;
				}

				os.write(buffer, 0, bytesRead);
				nCount++;
				if (nCount % (100) == 0)
					System.out.println((new Date()).toLocaleString() + ": Have move " + nCount + " blocks");
			}

			is.close();
			os.close();
			System.out.println((new Date()).toLocaleString() + ": Write content of file " + file.getName() + " to hdfs file " + f.getName() + " success");

			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			log.info(e.getStackTrace());
		}
		return false;
	}

	/**
	 * 取得文件块所在的位置..
	 */
	public void getLocation()
	{
		try
		{
			Path f = new Path("/user/xxx/input02/file01");
			FileStatus fileStatus = fs.getFileStatus(f);

			BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for (BlockLocation currentLocation : blkLocations)
			{
				String[] hosts = currentLocation.getHosts();
				for (String host : hosts)
				{
					System.out.println(host);
				}
			}

			// 取得最后修改时间
			long modifyTime = fileStatus.getModificationTime();
			Date d = new Date(modifyTime);
			System.out.println(d);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 读取hdfs中的文件内容
	 */
	public boolean downloadFileFromHdfs(String hdfsFilename, String localPath)
	{
		try
		{
			Path f = new Path(hdfsFilename);

			FSDataInputStream dis = hdfs.open(f);
			File file = new File(localPath + "/" + f.getName());
			FileOutputStream os = new FileOutputStream(file);

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int nCount = 0;
			while ((length = dis.read(buffer)) > 0)
			{
				nCount++;
				os.write(buffer, 0, length);
				nTotalLength += length;
				if (nCount % 100 == 0)
					System.out.println((new Date()).toLocaleString() + ": Have move " + (nTotalLength / 1024000) + " MB");
			}

			os.close();
			dis.close();

			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public long getSizeOfPath(String path, boolean deepfind)
	{
		long sumSize = 0;
		try
		{
			if (!checkDirExist(path))
			{
				return -1;
			}

			List<FileStatus> fileList = listFileStatus(path, deepfind);
			for (FileStatus fileStatus : fileList)
			{
				if (fileStatus.getPath().toString().toLowerCase().endsWith(".gz"))
				{
					sumSize += fileStatus.getLen() * 25;
				}
				else if (fileStatus.getPath().toString().toLowerCase().endsWith(".deflate"))
				{
					sumSize += fileStatus.getLen() * 5;
				}
				else
				{
					sumSize += fileStatus.getLen();
				}
			}
		}
		catch (Exception e)
		{
			sumSize = -1;
		}
		return sumSize;
	}

	public long getSizeOfPath(String path, boolean deepfind, String filter)
	{
		long sumSize = 0;
		try
		{
			if (!checkDirExist(path))
			{
				return -1;
			}

			List<FileStatus> fileList = listFileStatus(path, deepfind);
			String[] vct = filter.split(",");
			for (FileStatus fileStatus : fileList)
			{
				boolean bFlag = false;
				if (filter.length() > 0)
				{
					for (String key : vct)
					{
						if (fileStatus.isFile() && fileStatus.getPath().getName().contains(key))
						{
							bFlag = true;
							break;
						}
					}
				}
				else
				{
					bFlag = true;
				}
				if (bFlag)
				{
					if (fileStatus.getPath().toString().toLowerCase().endsWith(".gz"))
					{
						sumSize += fileStatus.getLen() * 25;
					}
					else if (fileStatus.getPath().toString().toLowerCase().endsWith(".deflate"))
					{
						sumSize += fileStatus.getLen() * 5;
					}
					else
					{
						sumSize += fileStatus.getLen();
					}
				}
			}
		}
		catch (Exception e)
		{
			sumSize = -1;
		}
		return sumSize;
	}

	public FileStatus getFileStatus(String path)
	{
		System.out.println("getFileStatus：" + path);
		try
		{
			Path curPath = new Path(path);
			if (hdfs.exists(curPath) && (!curPath.isRoot()))
			{
				FileStatus fileStatus[] = fs.listStatus(curPath.getParent());
				int listlength = fileStatus.length;
				for (int i = 0; i < listlength; i++)
				{
					if (fileStatus[i].getPath().toString().contains(curPath.toString()))
					{
						System.out.println("getFileStatus：Success");
						return fileStatus[i];
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public List<FileStatus> listFlolderStatus(String path) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		List<FileStatus> resList = new ArrayList<FileStatus>();
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		for (int i = 0; i < listlength; i++)
		{
			if (fileStatus[i].isDirectory() == true)
			{
				resList.add(fileStatus[i]);
			}
		}
		return resList;
	}

	public List<FileStatus> listFileStatus(String path, boolean deepfind) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		List<FileStatus> resList = new ArrayList<FileStatus>();
		listFileStatus(path, deepfind, resList);
		return resList;
	}

	public void listFileStatus(String path, boolean deepfind, List<FileStatus> resList) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fs.listStatus(new Path(path));
		int listlength = fileStatus.length;
		for (int i = 0; i < listlength; i++)
		{
			if (fileStatus[i].isDirectory() == false)
			{
				resList.add(fileStatus[i]);
			}
			else
			{
				if (deepfind)
				{
					String newpath = fileStatus[i].getPath().toString();
					listFileStatus(newpath, deepfind, resList);
				}
			}
		}
	}

	public boolean mergeDirFiles(String srcDirPath)
	{
		return mergeDirFiles(srcDirPath, "\n");
	}

	public boolean mergeDirFiles(String srcDirPath, String rowTerminateFlag)
	{
		try
		{
			String tarPath = srcDirPath + "/mergefile";
			if (checkFileExist(tarPath))
			{
				int count = 1;
				String oldtarPath = "";
				do
				{
					oldtarPath = tarPath + "_" + count;
					count++;
				} while (checkFileExist(oldtarPath));
				movefile(tarPath, oldtarPath);
			}

			List<FileStatus> fileList = listFileStatus(srcDirPath, true);
			if (fileList.size() > 1)
			{
				if (mergeDirFiles(fileList, tarPath, rowTerminateFlag))
				{
					for (FileStatus file : fileList)
					{
						delete(file.getPath().toString());
					}
				}
			}

		}
		catch (Exception e)
		{
			return false;
		}
		return true;

	}

	public boolean mergeDirFiles(String srcDirPath, String tarPath, String rowTerminateFlag)
	{
		try
		{
			System.out.println(" merge dir files into one : " + srcDirPath + " to " + tarPath);
			if (!checkDirExist(srcDirPath))
			{
				return false;
			}
			List<FileStatus> fileList = listFileStatus(srcDirPath, true);
			for (FileStatus file : fileList)
			{
				if (file.getPath().equals(tarPath))
				{
					System.out.println(" merge source dir can not cantain target file :  " + srcDirPath + " to " + tarPath);
					return false;
				}
			}
			return mergeDirFiles(fileList, tarPath, rowTerminateFlag);
		}
		catch (Exception e)
		{
			return false;
		}
	}

	public boolean mergeDirSmallFiles(String srcDirPath, String rowTerminateFlag, long sizelimit)
	{
		try
		{
			String tarPath = srcDirPath + "/mergefile";

			if (!checkDirExist(srcDirPath))
			{
				System.out.println(" merge source dir path is not exists : " + srcDirPath);
				return false;
			}

			if (checkFileExist(tarPath))
			{
				int count = 1;
				String oldtarPath = "";
				do
				{
					oldtarPath = tarPath + "_" + count;
					count++;
				} while (checkFileExist(oldtarPath));
				movefile(tarPath, oldtarPath);
			}

			List<FileStatus> fileList = listFileStatus(srcDirPath, true);
			List<FileStatus> mergeFileList = new ArrayList<FileStatus>();
			for (FileStatus file : fileList)
			{
				if (file.getLen() <= sizelimit)
				{
					mergeFileList.add(file);
				}
			}

			if (mergeFileList.size() > 1)
			{
				if (mergeDirFiles(mergeFileList, tarPath, rowTerminateFlag))
				{
					for (FileStatus file : mergeFileList)
					{
						delete(file.getPath().toString());
					}
				}
			}

		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	public boolean mergeDirFiles(List<FileStatus> fileList, String tarPath, String rowTerminateFlag)
	{

		FSDataOutputStream tarFileOutputStream = null;
		FSDataInputStream srcFileInputStream = null;

		try
		{
			Path tarFile = new Path(tarPath);
			tarFileOutputStream = fs.create(tarFile, true);

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int nCount = 0;
			boolean bfirst = true;
			for (FileStatus file : fileList)
			{
				if (file.getPath().equals(tarFile))
				{
					continue;
				}
				System.out.println(" merging file from  " + file.getPath() + " to " + tarPath);

				if (!bfirst)
				{
					// 添加换行符
					tarFileOutputStream.write(rowTerminateFlag.getBytes(), 0, rowTerminateFlag.length());
				}

				srcFileInputStream = hdfs.open(file.getPath(), buffer.length);
				while ((length = srcFileInputStream.read(buffer)) > 0)
				{
					nCount++;
					tarFileOutputStream.write(buffer, 0, length);
					nTotalLength += length;
					// System.out.println(" file length " + file.getLen() + "
					// read " + length);
					if (nCount % 1000 == 0)
					{
						tarFileOutputStream.flush();
						System.out.println((new Date()).toLocaleString() + ": Have move " + (nTotalLength / 1024000) + " MB");
					}

				}

				srcFileInputStream.close();

				bfirst = false;
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
			try
			{
				delete(tarPath);
			}
			catch (Exception e2)
			{
				// TODO: handle exception
			}
			return false;
		}
		finally
		{
			try
			{
				if (tarFileOutputStream != null)
				{
					tarFileOutputStream.flush();
					tarFileOutputStream.close();
					srcFileInputStream.close();
				}
			}
			catch (Exception e2)
			{
				// TODO: handle exception
			}

		}
		return true;
	}

	public boolean mergeFiles(String srcPath, String tarPath, String rowTerminateFlag)
	{
		try
		{
			Path srcFile = new Path(srcPath);
			FSDataInputStream srcFileInputStream = hdfs.open(srcFile);

			Path tarFile = new Path(tarPath);
			FSDataOutputStream tarFileOutputStream = fs.create(tarFile, true);
			tarFileOutputStream.write(rowTerminateFlag.getBytes(), 0, rowTerminateFlag.length());

			byte[] buffer = new byte[1024000];
			int length = 0;
			long nTotalLength = 0;
			int nCount = 0;
			while ((length = srcFileInputStream.read(buffer)) > 0)
			{
				nCount++;
				tarFileOutputStream.write(buffer, 0, length);
				nTotalLength += length;
				if (nCount % 100 == 0)
					System.out.println((new Date()).toLocaleString() + ": Have move " + (nTotalLength / 1024000) + " MB");
			}

			tarFileOutputStream.flush();
			tarFileOutputStream.close();
			srcFileInputStream.close();

			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public boolean CreateEmptyFile(String filename)
	{
		try
		{
			Path f = new Path(filename);
			FSDataOutputStream os = fs.create(f, true);
			os.close();
			return true;
		}
		catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	public void writerString(String text, String path)
	{

		try
		{
			Path f = new Path(path);
			FSDataOutputStream os = fs.create(f, true);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));// 以UTF-8格式写入文件，不乱码
			writer.write(text);
			writer.close();
			os.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();

		}

	}

	/**
	 * 获取hdfs文件的输入流
	 * 
	 * @param hdfsFilename
	 * @return
	 */
	public FSDataInputStream getInputStream(String hdfsFilename)
	{
		FSDataInputStream dis = null;
		try
		{
			Path f = new Path(hdfsFilename);

			dis = hdfs.open(f);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return dis;
	}

	public static void main(String[] args)
	{
		try
		{
			// System.out.println("usage:HdfsPuter localFilename hdfsPath");
			// HDFSOper hdfsOper = new HDFSOper("hdfs://10.139.6.169:9000");
			//
			// boolean isFileExists =
			// hdfsOper.checkFileExist("/mt_wlyh/Data/mroxdrmerge/mro_loc/cai_output_01_151002_2");
			//
			// hdfsOper.movefile("/mt_wlyh/Data/mroxdrmerge/mro_loc/output_01_151002_1",
			// "/mt_wlyh/Data/mroxdrmerge/mro_loc/cai_output_01_151002_1");

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

}
