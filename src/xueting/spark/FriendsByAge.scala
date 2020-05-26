package xueting.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseline(line: String) = {
    // split by commas
    val fields = line.split(",")
    // extract age and numFriendsfields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // create a tuple that is our result
    (age, numFriends)
  }
  
  
  /**Our main function where the action begins*/
  def main(args: Array[String]) {
    
    // set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")
    
    // load each line of source data into an RDD
    val lines = sc.textFile("data/fakefriends.csv")
    
    // convert to (age, numFriends) tuple using 'parseline' function
    val rdd = lines.map(parseline)
    
    // Start with an RDD of form (age, numFriends), age-> numFriends
    // Use mapValues() to convert each numFriends value ro a tuple of (numFriends, 1)
    // then use reduceByKey() to sum up (# of numFriends) & (# of instances for each age)
    // by adding together all numFriends values and 1's respectively.
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )
    
    // now, we have tuples of (age, (totalFriends, totalInstances))
    // to compute the average, we divide totalFriends / totalInstances for each age
    val avgByAge = totalsByAge.mapValues( x => x._1 / x._2 )
    
    // collect results from RDD
    // this is an action, will compute the DAG and actually execute the job
    val results = avgByAge.collect()
    
    // sort by age (_._1) and print the final results
    results.sortBy(_._1).foreach(println)
    

  } 
}
  