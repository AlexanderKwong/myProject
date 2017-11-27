package logic.spark.mroLoc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/27
 */
public class MroLableFillMain {

    public static boolean runSparkJob(JavaSparkContext sc){


        JavaPairRDD<Object, Text> xdrRDD = sc.newAPIHadoopFile("", CombineTextInputFormat.class, Object.class, Text.class, sc.hadoopConfiguration());

        JavaPairRDD<Object, Text> mrRDD = sc.newAPIHadoopFile("", CombineTextInputFormat.class, Object.class, Text.class, sc.hadoopConfiguration());

        SQLContext sqlContext = new SQLContext(sc);

//        sqlContext.createDataFrame();
        //TODO RDD TO DataFrame

        DataFrame xdrDF = null;

        DataFrame mrDF = null;

        mrDF.join(xdrDF, new Column("eci"), "LEFT");

        return false;
    }
}
