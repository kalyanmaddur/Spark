package spark60days

import org.apache.spark.sql.SparkSession

object day_5 {
  
  //Dynamic Shuffling in Spark
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().master(master = "local[*]").appName(name="day_5").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    val df = spark.read.option("header","false").csv(path= "file:///C:/Users/DELL/workspace/Spark/datasets/customers-orders.csv")
    df.show(truncate=false)
    println(df.rdd.partitions.length) //output of this lenght = 1
    
    println(df.groupBy("_c0").count().rdd.partitions.length) //output = 200
    // _c0 --> first column in df
    //default value of partition length = 200
    
    spark.conf.set("spark.sql.shuffle.partitions",100)
    
    println(df.groupBy("_c0").count().rdd.partitions.length)
  }
}