import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import StructData.SIGNAL_MR_All;
import cellconfig.CellConfig;
import cellconfig.LteCellConfig;
import cellconfig.LteCellInfo;
import jan.com.hadoop.hdfs.HDFSOper;
import jan.util.DataAdapterConf.ParseItem;
import jan.util.DataAdapterReader;
import jan.util.IWriteLogCallBack.LogType;
import jan.util.LOGHelper;
import locuser.UserProp;
import mroxdrmerge.MainModel;

public class GetData
{
	public static void main(String[] args) throws Exception
	{	
		Configuration conf = new Configuration();
		String fsurl = "hdfs://" + MainModel.GetInstance().getAppConfig().getHadoopHost() + ":"
				+ MainModel.GetInstance().getAppConfig().getHadoopHdfsPort();
		conf.set("fs.defaultFS", fsurl);
		// 初始化小区的信息
		if (!CellConfig.GetInstance().loadLteCell(conf))
		{
			LOGHelper.GetLogger().writeLog(LogType.error, "cellconfig init error 请检查！");
			throw (new IOException("cellconfig init error 请检查！"));
		}
		
		
	    HDFSOper hdfsOper = new HDFSOper(conf);
	    
		BufferedReader reader = null;
    	String filePath = "hdfs://192.168.1.31:9000/mt_wlyh/Data/mromt/161013/data_2016101310";
		reader = new BufferedReader(new InputStreamReader(hdfsOper.getHdfs().open(new Path(filePath)), "UTF-8"));
		
		String strData;
		String[] strs;
		ParseItem parseItem;
		DataAdapterReader dataAdapterReader;
		parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MRO-SRC-UE");
		if (parseItem == null)
		{
			throw new IOException("parse item do not get.");
		}
		dataAdapterReader = new DataAdapterReader(parseItem);
		Map<Long,List<SIGNAL_MR_All>> mrMap = new HashMap<Long,List<SIGNAL_MR_All>>();
		
		String path="C:/Users/Jancan/Desktop/testdata.txt";
        File file=new File(path);
        if(file.exists())
        {
        	file.delete();
        }
        file.createNewFile();
        FileOutputStream fileOutputStream=new FileOutputStream(file,false);
        StringBuffer sb=new StringBuffer();
        
        Set<Integer> iset = new  HashSet<Integer>();
        iset.add(47154433);
        iset.add(47154434);
        iset.add(47154435);
        iset.add(47154436);
        iset.add(47154437);
        iset.add(47154438);
        iset.add(47154439);
        iset.add(47154440);
        iset.add(47154441);
        iset.add(47154442);
        iset.add(47154443);
        iset.add(47154444);
        iset.add(47154445);
        iset.add(47154446);
        iset.add(47154447);
        iset.add(47154448);
        iset.add(47154449);
        iset.add(47154450);
        
        Map<Long, List<String>> cellData = new HashMap<Long, List<String>>();
		
		while((strData = reader.readLine()) != null)
		{
			strs = strData.split(parseItem.getSplitMark(), -1);
			dataAdapterReader.readData(strs);
			
			StructData.SIGNAL_MR_All mrData = new StructData.SIGNAL_MR_All();
			try
			{
				mrData.FillData(dataAdapterReader);
			}
			catch (Exception e)
			{
				continue;
			}
			
			LteCellInfo cell = CellConfig.GetInstance().getLteCell(mrData.tsc.Eci);
			if(cell != null && cell.indoor == 1)
			{
				List<String> strList = cellData.get(mrData.tsc.Eci);
				if(strList == null)
				{
					strList = new ArrayList<String>();
					cellData.put(mrData.tsc.Eci, strList);
				}
				strList.add(strData);
				
				if(strList.size() > 1000)
				{
					for (String str : strList)
					{
						sb.delete(0, sb.length());
						sb.append(str + "\n");
						fileOutputStream.write(sb.toString().getBytes("utf-8"));
					}
					strList.clear();
				}
			}
		}

		fileOutputStream.close();
			
		
	}
	
	
	
}
