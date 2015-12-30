package it.mgaido.spark.io

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

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
  
}