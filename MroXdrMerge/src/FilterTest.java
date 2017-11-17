

/**
 * User: hadoop
 * Date: 2014/10/10 0010
 * Time: 19:26
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class FilterTest {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(final String[] args) throws Exception {

		if (args.length < 3) {
			System.err
					.println("Usage: FilterTest <filein> <fileoutput> <keyword> [type]");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(
				"JavaWordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		if(args.length > 3 && args[3].toLowerCase().contains("http"))
		{
			HttpFilter(lines, args);
		}
		else if(args.length > 3 && args[3].toLowerCase().contains("mme"))
		{
			MmeFilter(lines, args);
		}
		else
		{
			NormalFilter(lines, args);
		}
		ctx.stop();
	}

	private static void NormalFilter(JavaRDD<String> lines, final String[] args) {
		JavaRDD<String> logData1 = lines
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) {
						return s.contains(args[2]);
					}
				});

		logData1.saveAsTextFile(args[1]);

	}

	private static void MmeFilter(JavaRDD<String> lines, final String[] args) {

		JavaRDD<String> logData1 = lines
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) {
						String[] vct = s.split("\t");
						if (vct.length>=23 && vct[22].contains(args[2]))
							return true;
						return false;
					}
				});

		logData1.saveAsTextFile(args[1]);
	}
	
	private static void HttpFilter(JavaRDD<String> lines, final String[] args) {

		JavaRDD<String> logData1 = lines
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) {
						String[] vct = s.split("\t");
						if (vct.length>=9 && vct[8].contains(args[2]))
							return true;
						return false;
					}
				});

		logData1.saveAsTextFile(args[1]);
	}
}