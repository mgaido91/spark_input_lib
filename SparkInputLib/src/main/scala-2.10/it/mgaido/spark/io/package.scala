package it.mgaido.spark

/**
 * @author m.gaido
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

package object io {
  implicit class IOSparkContext(sparkContext: SparkContext){
  
    def textFileWithHeader(path:String, numHeaderLines:Int):RDD[String] = {
      IOHelper.readTextFilesWithHeader(sparkContext,path, numHeaderLines)
    }
  
  }
}

