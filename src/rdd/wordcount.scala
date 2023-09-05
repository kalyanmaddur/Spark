// No: 1

package rdd

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wordcount extends App {
  
  /*def main (args:Array[String]){ -- main method
      } 
  */
  val sc = new SparkContext("local[*]","word-count") //-- creating spark context
  
  
  /* scala and spark compatibility 
  * 2.4.2 spark -- 2.12 scala
  * 2.4.3/2.44  spark -- 2.11 scala 
  */
  // CTRL + SHIFT + O - to import relevant packages
  // Set build path before importing
  
  //setting the logger level to error 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val input = sc.textFile("file:///C:/Users/DELL/words.txt") //--Each line in file will be loaded as each value in array
                                                             // O/P --> Array[ (line1),(line2),(line3)]
  
  val words = input.flatMap(x => x.split(" ")) //takes each line as input and splits into words
                                               // O/P --> Array[ Array(Word1,word2),Array(word1,word2)]
  
  val words_lower = words.map(x => x.toLowerCase()) // _.toLowerCase() -- place holder syntax
  
  val words_count = words_lower.map(x => (x,1)) // .map((_,1)) -- place holder syntax
  
  val final_count = words_count.reduceByKey((x,y) => x+y)  // reduceByKey(_+_) -- place holder syntax
  
  final_count.collect.foreach(println) // --action
  
  scala.io.StdIn.readLine() // take input from user, this makes the DAG visible even 
                            // after the program is finished.
}