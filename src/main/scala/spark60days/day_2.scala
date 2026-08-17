package spark60days

import org.apache.spark.sql.SparkSession

object day_2 {
  
  //Read Text files into single RDD

  def main(args: Array[String]):Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("day_2").getOrCreate()
    
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    //wholeTextFiles returns a RDD[Tuple2]
    //where first value (_1) in the tuple is FILE NAME and second value (_2) is the content of the file
    
    val rdd = sc.wholeTextFiles("C:/Users/DELL/workspace/Spark/datasets/*")
    
    rdd.foreach(f => {
      println(f._1+ "=>"+f._2)
    })
    
  }
}
/*
Read Text files into single RDD
**********************************
textFile() & wholeTextFiles() methods are used to read single and multiple text or csv files into a single Spark RDD.

textFile() – Read single or multiple text, csv files and returns a single Spark RDD [String]

wholeTextFiles() – Reads single or multiple files and returns a single RDD[Tuple2[String, String]], where first value (_1) in a tuple is a file name and second value (_2) is content of the file.

We can use matching pattern while giving the path of directory.
eg. spark.sparkContext.textFile("D:/myData/text*.txt")
*/