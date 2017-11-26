package jan.com.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import base.IDataOutputer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutputMng<KEYOUT, VALUEOUT> implements IDataOutputer<KEYOUT, VALUEOUT> {
	private Configuration conf;
	private Map<String, MultiTypeItem> multiTypeMap;
	private DistributedFileSystem hdfs;

	private MultipleOutputs<KEYOUT, VALUEOUT> mos;

	public MultiOutputMng(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context, String fsUri) throws IOException {
		this.conf = context.getConfiguration();
		multiTypeMap = new HashMap<String, MultiTypeItem>();

		mos = new MultipleOutputs<KEYOUT, VALUEOUT>(context);
		if (fsUri.length() > 0) {
			FileSystem.setDefaultUri(conf, fsUri);
			FileSystem fs = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fs;
		}
		String mPath = conf.get("mapreduce.multipleoutputs");
		if (mPath != null) {
			String[] mPaths = mPath.trim().split(" ");
			for (String tmstr : mPaths) {
				String fileName = FileOutputFormat.getUniqueFile(context, tmstr, "");

				MultiTypeItem item = new MultiTypeItem();
				item.typeName = tmstr;
				item.fileName = fileName;
				multiTypeMap.put(item.typeName, item);
			}
		}
	}

	public void init() throws IOException {
		for (String item : multiTypeMap.keySet()) {
			// System.out.println(item);
			if (multiTypeMap.get(item) != null && multiTypeMap.get(item).basePath == null) {
				// System.out.println(multiTypeMap.get(item).basePath);
			}
		}

		// if(hdfs == null)
		// return;

		for (MultiTypeItem item : multiTypeMap.values())
		{
			try{
				// 需要检查是否已经存在
				if (item != null && item.basePath.length() > 0) {
					Path path = new Path(item.basePath, item.fileName);
					if (hdfs != null && hdfs.exists(path)) {
						hdfs.delete(path, false);
					}
				}
			}catch(Exception e){
				
			}
		}
	}

	public void SetOutputPath(String typeName, String path) {
		MultiTypeItem item = multiTypeMap.get(typeName);
		if (item != null) {
			item.basePath = path;
		}
	}

	public void write(String typeName, KEYOUT key, VALUEOUT value) throws Exception{
		MultiTypeItem item = multiTypeMap.get(typeName);
		if (item != null) {
			mos.write(typeName, key, value, item.getPathName());

		}
	}

	public void close() throws IOException, InterruptedException {
		mos.close();
	}

	private class MultiTypeItem {
		public String typeName;
		public String fileName;
		public String basePath;

		public MultiTypeItem() {
			typeName = "";
			fileName = "";
			basePath = "";
		}

		public String getPathName() {
			return basePath + "/" + typeName;
		}
	}

}
