// No: 2

package rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object top_words_in_file extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","top")
  
  val input = sc.textFile("file:///C:/Users/DELL/words.txt")
  
  val words = input.flatMap(x => x.split(" ")) // flatmap(_,.split(" ")) -- placeholder syntax
  
  val words_lower = words.map(x => x.toLowerCase()) //map o/p is tuple
  
  val words_count  = words_lower.map(x => (x,1))
  
  val final_count = words_count.reduceByKey((x,y) => x+y)
  
  println("================word count========================")
  final_count.collect.foreach(println)
  
  
  val exchange_keys_and_values = final_count.map(x => (x._2,x._1)) // exchanging keys and value postions for sorting by value
  val words_sort = exchange_keys_and_values.sortByKey(false) // sorting by key, by default sorting is ascending, (false) is desceding order
  
  val original_keys_and_values = words_sort.map(x => (x._2,x._1))
  
  /*
   * insted of exchange the key, value postions and perform sorting using sortByKey 
   * we can directly sort on the column this eradicates the data shuffle in previous method
   *  val words_sort = exchange_keys_and_values.sortBy(x => x._2)
   *  this sorts directly on value 
   */
  val top_10_words = original_keys_and_values
  println("=================top 10 words======================")
  //top_10_words.collect.foreach(println)
  
  for (result <- top_10_words){
    val word = result._1
    val count = result._2
    println(s"$word : $count")
    
    
  }
}