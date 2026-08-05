// No: 1

package rdd

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wordcount extends App {
  
  //App trait is used instead of main method, No explicit main method is needed.Instead, the whole class body becomes the “main method”.
  
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
  
  //setting the logger level to error, this does not print the information messages but prints only the errors and output in logs 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val input = sc.textFile("file:///C:/Users/DELL/workspace/Spark/datasets/words.txt") //--Each line in file will be loaded as each value in array
                                                             // O/P --> Array[ (line1),(line2),(line3)]
                                                             // "file:///" -- indicates local path.
  
  val words = input.flatMap(x => x.split(" ")) //takes each line as input and splits into words
                                               //O/P --> Array[ Array(Word1,word2),Array(word1,word2)]
                                               //using split on a string creates a Array of strings.
                                               //Flattening occurs on the result of the split func, thus converting the nestng collection to normal collection.
                                               //final O/P --> Array[(Word1),(Word2),(Word3),....]
                                               //flatmap = map + flatten -- map each element  + flatten one level
  
  
  val words_lower = words.map(x => x.toLowerCase()) // _.toLowerCase() -- place holder syntax
  
  val words_count = words_lower.map(x => (x,1)) // .map((_,1)) -- place holder syntax -- O/P --> Array[(Word1,1),(Word2,1),(Word3,1),.....] --> RDD of Tuples -- Pair RDD
  
  val final_count = words_count.reduceByKey((x,y) => x+y)  // reduceByKey(_+_) -- place holder syntax
                                                           // _+_ equal to  (a,b) => a+b
                                                           //reducebykey only takes two values associated with the same key.“it takes two inputs, not keys.”
  
  final_count.collect.foreach(println) // --action
  
  //Incase in the absence of spark history server, spark web UI is inaccessible as soon as the job is completed.
  
  scala.io.StdIn.readLine() // take input from user, this makes the DAG visible even after the program is finished as the program waits for the user input before completion.
  
}