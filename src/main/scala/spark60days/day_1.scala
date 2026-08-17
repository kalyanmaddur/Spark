package spark60days

import org.apache.spark.sql.SparkSession

object day_1 {
  
// creating rdd from parallelized collections  
  def main (args: Array[String]):Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("day_1").getOrCreate()
    
    val sc = spark.sparkContext
    
    sc.setLogLevel("ERROR")
    
    val rdd = sc.parallelize(List(1,2,3,4))
    
    val partition = rdd.getNumPartitions
    
    println(s"Number of partitions of rdd $partition")
    
    val first = rdd.first()
    
    println(s"first element of rdd $first")
  }
  
}