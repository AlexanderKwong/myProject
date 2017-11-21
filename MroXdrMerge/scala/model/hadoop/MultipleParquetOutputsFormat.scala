// SPECIFIC MULTIPLE OUTPUT FORMAT CLASSES

// For Parquet
package model.hadoop

import org.apache.avro.generic.GenericContainer
import parquet.hadoop.ParquetOutputFormat

class MultipleParquetOutputsFormat[T <: GenericContainer]
  extends MultipleOutputsFormat (new ParquetOutputFormat[T]) {
}