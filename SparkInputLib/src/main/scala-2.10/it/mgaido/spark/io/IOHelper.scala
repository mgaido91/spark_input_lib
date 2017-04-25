package it.mgaido.spark.io

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

/**
 * @author m.gaido
 */
object IOHelper {
  def readTextFilesWithHeader(path:String, numHeaderLines:Int)(implicit sparkContext:SparkContext):RDD[String] = {
    readTextFilesWithHeader(sparkContext, path, numHeaderLines)
  }
  
  def readTextFilesWithHeader(sparkContext:SparkContext, path:String, numHeaderLines:Int):RDD[String] = {
    sparkContext.hadoopConfiguration.setInt(InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES, numHeaderLines)
    sparkContext.newAPIHadoopFile[LongWritable, Text, FileWithHeaderReader](path)
          .map(line => line._2.toString)
  }
  
  def readHbaseTable(tableName:String, startRow:Option[String] = None,
                     endRow:Option[String] = None):RDD[(ImmutableBytesWritable, Result)] = {
    @transient val hconf = HBaseConfiguration.create()
    hconf.set(TableInputFormat.INPUT_TABLE, tableName)
    if(startRow.isDefined){
      hconf.set(TableInputFormat.SCAN_ROW_START, startRow.get)
    }
    if(endRow.isDefined){
      hconf.set(TableInputFormat.SCAN_ROW_STOP, endRow.get)
    }
    val job = Job.getInstance(hconf)
    job.setInputFormatClass(classOf[TableInputFormat])
    
    @transient val jobConf = job.getConfiguration
    jobConf.set(TableInputFormat.INPUT_TABLE, tableName)
    SparkContext.getOrCreate().newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
  }
  
  def upsertToHbaseTable(tableName:String, rdd:RDD[(ImmutableBytesWritable, Put)]) = {
    @transient val hconf = HBaseConfiguration.create()
    hconf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    
    val job = Job.getInstance(hconf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    @transient val jobConf = job.getConfiguration
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    rdd.saveAsNewAPIHadoopDataset(jobConf)
  }
  
}