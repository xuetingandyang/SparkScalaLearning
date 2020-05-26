package xueting.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PurchaseByCustomer {
  /* Count the total amount spent for each customer */
  
  def main(args: Array[String]) {
    
    // set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // create a SparkContext using each core of local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")
    
    // read lines from input data
    val lines = sc.textFile("data/customer-orders.csv")
    
    // convert to (customerId, amount) tuples 
    // {} is an expression. What we returned is the '()' at the end of expression
    val customerAmount = lines.map( x => {val y = x.split(","); (y(0).toInt, y(2).toFloat)} )
    
    // reduce by adding the amount together for each user
    val customerTotal = customerAmount.reduceByKey( (x, y) => x + y )

    // collect() results 
    val results = customerTotal.collect()
    
    // print results by sorted total amount
    results.sortBy(_._2).foreach(println)
    

    
  }
  
}