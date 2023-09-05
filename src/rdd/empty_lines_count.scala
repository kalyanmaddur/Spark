// No : 10

// counting empty lines in a file using accumulators

package rdd

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object empty_lines_count extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","empty lines")
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/sample-file.txt")
  
  val acc = sc.longAccumulator("empty_lines accumulators") // creating a longaccumulator
  
  input.foreach(x => if(x == "") acc.add(1))  // checking if the input line is empty, if empty adding 1 value to the accumulator to the count of empty lines in the file
  
  println("accumulator : " + acc)
  
  println("accumulator name :" + acc.name)
  
  println("no of empty lines : " + acc.value) // value gives the value of accumulator
  
  scala.io.StdIn.readLine()
  
  
}