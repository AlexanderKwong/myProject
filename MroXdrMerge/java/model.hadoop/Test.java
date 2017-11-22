package model.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progress;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/21
 */
public class Test implements Serializable{

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws IOException{
        new Test().test();
    }

//	public Configuration conf = new MyConf();

    public void test() throws IOException{
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Tuple2<String, String>> tmp = new ArrayList<>();
        tmp.add(new Tuple2<String, String>("KEY1", "VALUE1"));
        tmp.add(new Tuple2<String, String>("KEY2", "VALUE2"));
        tmp.add(new Tuple2<String, String>("KEY3", "VALUE3"));
        tmp.add(new Tuple2<String, String>("KEY4", "VALUE4"));
        tmp.add( new Tuple2<String, String>("KEY5", "VALUE5"));
        JavaPairRDD<String, String> rdd =  sc.parallelizePairs(tmp);

        /***************** test 1 ****************/
        /**
         * use new Configuration()
         */

//        conf.set("fs.defaultFS", "hdfs://192.168.1.31:9000");
//        Job job = Job.getInstance(conf);
//        MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, NullWritable.class, Text.class);
//        conf = job.getConfiguration();
        /***************** test 2 ****************/
        /**
         * use rdd.context().hadoopConfiguration()
         */
        final int stageId = rdd.id();

        Function<Tuple2<String, String>,Object> func = new Function<Tuple2<String, String>,Object>() {

            MultipleOutputs<NullWritable, Text> mos;
            String jobtrackerID = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());

            @Override
            public Object call(Tuple2<String, String> kv) throws Exception {
                if(mos == null){
                    initialize();
                }
//                Text tmpOut = new Text();
//            	tmpOut.set(kv._2);
//            	mos.write(NullWritable.get(), new Text(kv._2), "E:/tmp/out");
                mos.write("test", NullWritable.get(), new Text(kv._2), "E:/tmp/out");
                return null;
            }

            private void initialize(){
//
                Job job;
                Configuration conf_tmp = new Configuration();
                try
                {

                    job = Job.getInstance(conf_tmp);
                    MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, NullWritable.class, Text.class);
//					job.setOutputFormatClass(TextOutputFormat.class);
                    conf_tmp = job.getConfiguration();
                }
                catch (IOException e1)
                {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                ////////////////////

                TaskContext context = TaskContext.get();
                try{
                    //创建 MultipleOutputs
                    mos = new MultipleOutputs<NullWritable, Text>(
                            new ReduceContextImpl(conf_tmp, new TaskAttemptID(jobtrackerID, stageId, false, context.partitionId(), context.attemptNumber()), new DummyIterator(), new GenericCounter(), new GenericCounter(),
                                    new DummyRecordWriter(), new DummyOutputCommitter(), new TaskAttemptContextImpl.DummyReporter(), null,
                                    NullWritable.class, Text.class));
                }catch (Exception e){
//            	TaskContext context = TaskContext.get();
//              	TaskAttemptID attemptId = new TaskAttemptID(jobtrackerID, stageId, false, context.partitionId(), context.attemptNumber());
//              	TaskAttemptContextImpl  hadoopContext = new TaskAttemptContextImpl(conf_tmp, attemptId);
//              	TextOutputFormat tof = new TextOutputFormat<>();
//              	try{
//              		LazyOutputFormat.setOutputFormatClass(Job.getInstance(conf_tmp), TextOutputFormat.class);
//                      //创建 MultipleOutputs
//                      mos = new MultipleOutputs<NullWritable, Text>(
//                              new ReduceContextImpl(conf_tmp, attemptId, new DummyIterator(), new GenericCounter(), new GenericCounter(),
//                              		tof.getRecordWriter(hadoopContext), tof.getOutputCommitter(hadoopContext), new TaskAttemptContextImpl.DummyReporter(), null,
//                                      NullWritable.class, NullWritable.class));
//                  }catch (Exception e){
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };

        rdd.map(func).count();

        sc.stop();
//        Function2<Integer,Iterator, Iterator> func2 = null;
//        rdd.mapPartitionsWithIndex(func2, true).count();
//        try {
//            Job job = Job.getInstance(rdd.context().hadoopConfiguration());
//            job.setOutputKeyClass(NullWritable.class);
//            job.setOutputValueClass(Test.class);
//            job.setOutputFormatClass(outputFormatClass);
//            job.getConfiguration().set("mapred.output.dir", path);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        rdd.saveAsNewAPIHadoopFile();
    }


    private class DummyOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {

        }

        @Override
        public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

        }

        @Override
        public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

        }
    }

    private class DummyRecordWriter<K, V> extends RecordWriter<K, V> {

        @Override
        public void write(K k, V v) throws IOException, InterruptedException {
            System.out.println("------------key : " + k + "---value : " + v +"-------------");
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }

    private class DummyIterator implements RawKeyValueIterator {

        @Override
        public DataInputBuffer getKey() throws IOException {
            return null;
        }

        @Override
        public DataInputBuffer getValue() throws IOException {
            return null;
        }

        @Override
        public boolean next() throws IOException {
            return false;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public Progress getProgress() {
            return null;
        }
    }

    private class MyConf extends Configuration implements Serializable{

    }
}
