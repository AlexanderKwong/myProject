// These extensions show how to read and write Avro RDDs and use MultipleOutputsFormat

package model.spark.extensions

import model.hadoop.{MultipleAvroOutputsFormat, MultipleParquetOutputsFormat, MultipleTextOutputsFormat}
import model.util.TextLike
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.avro.specific.SpecificData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.ParquetOutputFormat

import scala.reflect.{ClassTag, _}

object SerializationExtensions {

  val logger = Logger.getLogger(getClass)

  def avroParquetJob[T: ClassTag](job: Job = Job.getInstance(new Configuration())): Job = {
    val schema: Schema = SpecificData.get.getSchema(classTag[T].runtimeClass)
    AvroJob.setInputKeySchema(job, schema)
    AvroJob.setOutputKeySchema(job, schema)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setEnableDictionary(job, false)
    AvroParquetOutputFormat.setSchema(job, schema)
    job
  }

  def definedOrLog(record: GenericRecord, field: String): Boolean = {
    if (record.get(field) != null) return true
    logger.warn(s"Expected field '$field' to be defined, but it was not on record of type '${record.getClass}'")
    false
  }

  implicit class TextKeyRDD[K, V: TextLike](rdd: RDD[(K, V)]) {
    def saveAsMultipleTextFiles(outputKeyFunction: (K) => String, outputPath: String): Unit = {
      rdd.map{case (k,v) => ((outputKeyFunction(k), NullWritable.get),v)}.map {
        case (k, v: String) => (k, new Text(v))
        case (k, v: Array[Byte]) => (k, new Text(v))
        case (k, v: Text) => (k, new Text(v))
      }.saveAsNewAPIHadoopFile[MultipleTextOutputsFormat](outputPath)
    }
  }

  implicit class AvroRDDSparkContext(val sparkContext: SparkContext) extends AnyVal {
    def avroFile[T: ClassTag](path: String): RDD[T] = {
      sparkContext.newAPIHadoopFile(path,
        classOf[AvroKeyInputFormat[T]],
        classOf[AvroKey[T]],
        classOf[NullWritable],
        avroParquetJob().getConfiguration)
        .map[T](_._1.datum())
    }
  }

  implicit class AvroRDD[T <: GenericRecord : ClassTag](val avroRDD: RDD[T]) {
    /**
      * Filters Avro records with certain fields not defined (are null) and logs the fact
      */
    def filterIfUnexpectedNull(fields: String*): RDD[T] = {
      avroRDD.filter(r => fields.forall(definedOrLog(r, _)))
    }

    def saveAsParquetFile(outputPath: String): Unit = {
      avroRDD.map((null, _))
        .saveAsNewAPIHadoopFile(outputPath,
          classOf[Null],
          classTag[T].runtimeClass,
          classOf[ParquetOutputFormat[T]],
          avroParquetJob[T]().getConfiguration)
    }

    def saveAsMultipleParquetFiles(outputKeyFunction: (T) => String, outputPath: String): Unit = {
      avroRDD.map(r => ((outputKeyFunction(r), null), r))
        .saveAsNewAPIHadoopFile(outputPath,
          classOf[Null],
          classTag[T].runtimeClass,
          classOf[MultipleParquetOutputsFormat[T]],
          avroParquetJob[T]().getConfiguration)
    }

    def saveAsAvroFile(outputPath: String): Unit = {
      avroRDD.map(r => (new AvroKey[T](r), NullWritable.get))
        .saveAsNewAPIHadoopFile(outputPath,
          classOf[AvroKey[T]],
          classOf[NullWritable],
          classOf[AvroKeyOutputFormat[T]],
          avroParquetJob[T]().getConfiguration)
    }

    def saveAsMultipleAvroFiles(outputKeyFunction: (T) => String, outputPath: String): Unit = {
      avroRDD.map(r => ((outputKeyFunction(r), new AvroKey(r)), NullWritable.get))
        .saveAsNewAPIHadoopFile(outputPath,
          classOf[AvroKey[(String, T)]],
          classOf[NullWritable],
          classOf[MultipleAvroOutputsFormat[T]],
          avroParquetJob[T]().getConfiguration)
    }
  }

}