package it.mgaido.spark.io

import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/**
 * @author m.gaido
 */
class Tester extends FunSuite {
 
  lazy val conf=new SparkConf().setMaster("local[*]").setAppName("Test")//.set("DebugOutputPath", "/Users/mark9/debug/debug")
  lazy val sc=new SparkContext(conf)
  
  
  
  /*test("Testing Spark"){
    val rdd=sc.parallelize(Array(1,2,3,4))
    assert(rdd.reduce(_+_)==10)
  }*/
  
  test("Testing InputFileWithHeaderReader with valid values"){
    sc.hadoopConfiguration.setInt(InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES, 1);
    val lines = sc.newAPIHadoopFile[LongWritable,Text,FileWithHeaderReader]("test-files/").map(x=>x._2.toString)
    //val lines = sc.textFile("test-files/")
    assert(lines.count() == 4)
    
    lines.collect().foreach { x => println(x) }
    
    sc.hadoopConfiguration.setInt(InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES, 2);
    val lines2 = sc.newAPIHadoopFile[LongWritable,Text,FileWithHeaderReader]("test-files/").map(x=>x._2.toString)
    assert(lines2.count() == 2)
    lines2.collect().foreach { x => println(x) }
    
    sc.hadoopConfiguration.setInt(InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES, 3);
    val lines3 = sc.newAPIHadoopFile[LongWritable,Text,FileWithHeaderReader]("test-files/").map(x=>x._2.toString)
    assert(lines3.count() == 0)
    lines3.collect().foreach { x => println(x) }
  }
  
  
  test("Testing InputFileWithHeaderReader with invalid values"){
    sc.hadoopConfiguration.setInt(InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES, -1);
    val lines = sc.newAPIHadoopFile[LongWritable,Text,FileWithHeaderReader]("test-files/").map(x=>x._2.toString)
    assert(lines.count() == 6)
    
    lines.collect().foreach { x => println(x) }
    
    sc.hadoopConfiguration.setInt(InputFileWithHeaderReader.HEADER_NUMBER_OF_LINES, 100);
    val lines2 = sc.newAPIHadoopFile[LongWritable,Text,FileWithHeaderReader]("test-files/").map(x=>x._2.toString)
    assert(lines2.count() == 0)
    lines2.collect().foreach { x => println(x) }
    
    
  }
  
  
  
  
}