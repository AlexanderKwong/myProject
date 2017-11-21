package model.hadoop

import java.io.File
import java.util.UUID

import com.google.common.io.Files
import avro.UUIDUtils
import avro.geoip.{Lookup, Subdivision}
import avro.gulp.UsageFragment
import model.spark.extensions.SerializationExtensions._
import model.spark.extensions.SparkConfigurationExtensions._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._

object TestUtil {

  def mkPath(ks: Any*): String = {
    def keyMap(k: Any): String = k match {
      case xs: TraversableOnce[_] => xs.mkString(",")
      case id: avro.UUID => UUIDUtils.toJavaUUID(id).toString
      case x => x.toString
    }

    ks.map(keyMap).mkString("/")
  }
}

class MultipleOutputsFormatTest extends FunSuite with BeforeAndAfterAll {
  val localAvroFile = "/home/silas/ephemera/filtered-frag-delete.avro"
  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("Multiple Outputs Test")
    .configureAvroSerialization()
  val sc = new SparkContext(sparkConf)

  val outputPath = "tmp"
  val outputFile = new File(outputPath)

  outputFile.mkdirs()
  Files.fileTreeTraverser().postOrderTraversal(outputFile).asScala.foreach(_.delete)

  def usageFragment(user: String, lang: String, country: String): UsageFragment =
    UsageFragment.newBuilder()
      .setId(UUIDUtils.fromJavaUUID(UUID.fromString(user)))
      .setCloudUserId(UUIDUtils.fromJavaUUID(UUID.randomUUID()))
      .setSubmissionTimestamp(System.currentTimeMillis())
      .setEndTimestamp(System.currentTimeMillis() + 10000)
      .setEnabledLanguages(List(lang).asJava)
      .setGeoIP(Lookup.newBuilder()
        .setCountry(country)
        .setSubdivisions(List(Subdivision.newBuilder()
          .setName("foo")
          .build()).asJava).build()).build()

  val usageFragments = List[UsageFragment](
    usageFragment("3ebb0e0d-891e-4675-9ec9-3f3665195048","en_GB", "UK"),
    usageFragment("3ebb0e0d-891e-4675-9ec9-3f3665195048","fr_FR", "France"),
    usageFragment("3ebb0e0d-891e-4675-9ec9-3f3665195048","fr_FR", "France"),
    usageFragment("3ebb0e0d-891e-4675-9ec9-3f3665195048","fr_FR", "France"),
    usageFragment("ea7fbcce-98ea-4e44-97b2-d4783a95a700","en_GB", "UK"),
    usageFragment("ea7fbcce-98ea-4e44-97b2-d4783a95a700","es_LA", "UK"),
    usageFragment("ea7fbcce-98ea-4e44-97b2-d4783a95a700","es_LA", "UK"),
    usageFragment("ea7fbcce-98ea-4e44-97b2-d4783a95a700","es_LA", "UK"),
    usageFragment("af415c0a-aa72-4f91-8118-9e8eb377cfc2","es_EC", "Ecuador"),
    usageFragment("af415c0a-aa72-4f91-8118-9e8eb377cfc2","es_GT", "Guatemala"),
    usageFragment("af415c0a-aa72-4f91-8118-9e8eb377cfc2","es_GT", "Guatemala"),
    usageFragment("7f45d45c-2e10-4355-9ea7-69b99bbcd4fa","en_GB", "UK"),
    usageFragment("7f45d45c-2e10-4355-9ea7-69b99bbcd4fa","fr_FR", "UK"),
    usageFragment("ea7fbcce-98ea-4e44-97b2-d4783a95a700","en_GB", "UK")
  )

  test("Test multiple text outputs") {
    val values = sc.parallelize(List(
      ("fruit/items", "apple"),
      ("vegetable/items", "broccoli"),
      ("fruit/items", "pear"),
      ("fruit/items", "peach"),
      ("vegetable/items", "celery"),
      ("vegetable/items", "spinach")
    ))
    values.saveAsMultipleTextFiles(s => s, s"$outputPath/text")
  }

  test("Test multiple avro outputs") {
    val values = sc.parallelize(usageFragments)
    values.saveAsMultipleAvroFiles(uf =>
      TestUtil.mkPath(uf.getGeoIP.getCountry, uf.getCloudUserId),
      s"$outputPath/avro")
  }

  test("Test multiple parquet outputs") {
    val values = sc.parallelize(usageFragments)
    values.saveAsMultipleParquetFiles(uf =>
      TestUtil.mkPath(uf.getEnabledLanguages.asScala, uf.getCloudUserId, uf.getGeoIP.getCountry),
      s"$outputPath/parquet")
  }

  override protected def afterAll(): Unit = sc.stop()
}