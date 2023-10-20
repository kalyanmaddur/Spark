package spark60days

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object day_4 {
  
  //Repartition() vs Coalesce() in Spark
  def main(args:Array[String]) : Unit = {
    
    val spark = SparkSession.builder().master("local[*]").appName("day_4").getOrCreate()
    val sc = spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    val DF = spark.sparkContext.parallelize(Range(0,18),3).toDF()
    DF.show(false)
    val partitionNo = DF.rdd.getNumPartitions
    println(s"No of partitions in DF $partitionNo") //output:3
    DF.write.mode(SaveMode.Overwrite).csv("C:/Users/DELL/workspace/Spark/datasets/partitions_default")
    /*partition1 => 0,1,2,3,4,5
     *partition2 => 6,7,8,9,10,11
     *partition3 => 12,13,14,15,16,17
     */
    // Now repartitioning to 6 partitions
    val repartitionDF = DF.repartition(6)
    println(s"No of partitions in repartitionDF ${repartitionDF.rdd.getNumPartitions}") //output:6
    repartitionDF.write.mode(SaveMode.Overwrite).csv("C:/Users/DELL/workspace/Spark/datasets/repartitionDF")
    /*partition1 => 1,10,13
     *partition1 => 3,6,15 
     *partition1 => 0,8,16
     *partition1 => 5,9,17
     *partition1 => 2,11,12
     *partition1 => 4,7,14     
     */
    //Using coalesce to 2 partitions
    val coalesceDF = DF.coalesce(2)
    println(s"No of partitions in coalesceDF ${coalesceDF.rdd.getNumPartitions}")//output:2
    coalesceDF.write.mode(SaveMode.Overwrite).csv("C:/Users/DELL/workspace/Spark/datasets/coalesceDF")
    /*partition1 => 0,1,2,3,4,5
     *partition2 => 6,7,8,9,10,11,12,13,14,15,16,17 
     */
  }
}

/*
Spark repartition() vs coalesce() – repartition() is used to increase or decrease the RDD, Data Frame, Dataset partitions whereas the coalesce() is used to only 
decrease the number of partitions in an efficient way.

✅Repartition triggers a full shuffle of data and distributes the data evenly over the number of partitions and can be used to increase and decrease the partition count.
Coalesce is typically used for reducing the number of partitions and does not require a shuffle. According to the inline documentation of coalesce you can use coalesce to increase the number of partitions but you must set the shuffle argument to true. Please note that unlike repartition, coalesce does not guarantee equal partitions.

🔑 When to Use What and Why?
There is no general rule of thumb as to whether to use repartition or coalesce. Depending upon the transformations and the computations, repartition can be expensive 
as it involves a full reshuffle of the data across the cluster. Repartition also guarantees that the data distribution in the partition is roughly the same size. 
However, if data distribution is not a concern, then coalesce can be a good option to reduce the number of partitions as it avoids reshuffling, leading to 
faster computation but uneven data distribution in the partitions.
*/