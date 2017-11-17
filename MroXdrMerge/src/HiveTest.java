

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 使用反射的方式将RDD转换成为DataFrame Person [id=1, name=Spark, age=7] Person [id=2,
 * name=Hadoop, age=10]
 */
public class HiveTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"RDDToDataFrameByReflection");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(
				JavaSparkContext.toSparkContext(sc));

		// 读取数据

		DataFrame bigDatas = hiveContext
				.sql("select * from est_mt.tb_mro_format limit 10 ");

		// DataFrame => RDD
		JavaRDD<Row> bigDataRDD = bigDatas.javaRDD();

		JavaRDD<String> result = bigDataRDD.map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				// 返回具体每条记录
				return row.getString(2);
			}

		});

		List<String> personList = result.collect();
		for (String p : personList) {
			System.out.println(p);
		}

	}

}