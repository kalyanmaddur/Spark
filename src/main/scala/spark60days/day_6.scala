package spark60days

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object day_6 {
 
//shared variable -- broadcast variable
  def main (args: Array[String]): Unit = {
    
    val spark = SparkSession.builder.master("local[*]").appName("day_6").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    
    val df1 = spark.read.csv("C:/Users/DELL/workspace/Spark/datasets/customers-orders.csv");
    df1.show(false)
    val df2 = spark.read.csv("C:/Users/DELL/workspace/Spark/datasets/chapters-201108-004545.csv");
    df2.show(false)
   
    val joinDF = df1.join(broadcast(df2),df1("_c0") === df2("_c0"))  // broadcasting 2nd DF 
    
    spark.time()
    spark.time(joinDF.show)
  }
}

/*
 * Broadcast Variables in Spark
*********************************
✅ Broadcast variables allow the developers to cache a copy of the read-only variable on each machine/node rather than moving the copy of it with tasks. It means, that whenever the driver program encounters a broadcast variable(or one that can be broadcasted), it creates a copy of it and share it with all the machines/nodes where the codes are supposed to be executed.
✅ The benefit of copying the data using broadcast is that it resides on the worker node until the lifecycle of the Spark application. So if in multiple stages the same variable or data is used multiple times, it does not need to be copied every time and can be used from the cached data. The data is cached in a serialized form and when needed it can be de-serialized and used. It is always recommended to broadcast small-sized data(The default limit is 10MB) and this data should fit in the driver as well as executor memory.
✅ As we all know, in the case of joins, datasets are shuffled based on the join keys and if the smaller dataset is broadcasted, it helps reduce the expensive shuffle operations by broadcasting the smaller dataset to the partitions where the larger dataset resides and applies Broadcast Hash Join/Map Side Join on those partitions.
*/