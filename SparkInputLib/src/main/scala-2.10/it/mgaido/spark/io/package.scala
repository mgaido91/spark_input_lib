package it.mgaido.spark

/**
 * @author m.gaido
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put

package object io {
  implicit class IOSparkContext(sparkContext: SparkContext){
  
    def textFileWithHeader(path:String, numHeaderLines:Int):RDD[String] = {
      IOHelper.readTextFilesWithHeader(sparkContext,path, numHeaderLines)
    }
    
    def hbaseTable(tableName:String, startRow:Option[String] = None, endRow:Option[String] = None) = {
      IOHelper.readHbaseTable(tableName, startRow, endRow)
    }
  }
  
  implicit class HBaseRDD(rdd:RDD[(ImmutableBytesWritable, Put)]){
    def upsertToHbaseTable(tableName:String) = IOHelper.upsertToHbaseTable(tableName, rdd)
  }
}

