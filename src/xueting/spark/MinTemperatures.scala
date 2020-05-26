package xueting.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import scala.math.min

/** Find the minimum temperature by weather station */
object MinTemperatures {
 
  final case class Temperature(stationID: String, entryType: String, temperature: Float)
  
  def parselineDS(line: String): Temperature = {
    val fields = line.split(",")
    
    Temperature(fields(0), fields(2), fields(3).toFloat*0.1f*(9.0f/5.0f) + 32.0f)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("data/1800.csv").map(parselineDS)
    
    import spark.implicits._
    val tempDS = lines.toDS()
    tempDS.printSchema()
    
    // dataset.filter expects a column object, use "!==" and ===""
    // RDD.filter expects a Boolean object, can use "!=", "=="
    tempDS.filter(tempDS("entryType") === "TMIN").groupBy("stationID").min("temperature").show()
    
    spark.stop()
  }
  

/* Method2: Use RDD */  
//  def parseline(line: String) = {
//    val fields = line.split(",")
//    val stationID = fields(0)
//    val entryType = fields(2)
//    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
//    
//    (stationID, entryType, temperature)
//  }
//
//  /* Our main function where the action happens */
//  def main(args: Array[String]) {
//    // set the log evel to onlt print errors
//    Logger.getLogger("org").setLevel(Level.ERROR)
//  
//    // create a SparkContext using every core of the local machine
//    val sc = new SparkContext("local[*]", "MinTemperatures")
//    
//    // read each line of the input data
//    val lines = sc.textFile("data/1800.csv")
//    
//    // convert to (stationID, entryType, temperature) tuples
//    val parselines = lines.map(parseline)    
//    
//    // Filter out all but TMIN entries
//    val minTemps = parselines.filter( x => x._2 == "TMIN" )
//    
//    // Convert to (stationID, temperature) tuples
//    val stationTemps = minTemps.map( x => (x._1, x._3.toFloat) )
//    
//    // Reduce by stationID retaining the minimum temperature found
//    val minTempsByStation = stationTemps.reduceByKey( (x, y) => min(x, y) )
//    
//    // Collect, format, and print the results
//    val results = minTempsByStation.collect()
//    
//    for (result <- results.sorted) {
//      val station = result._1
//      val temp = result._2
//      val formattedTemp = f"$temp%.2f F"
//      println(s"$station minimum temperature: $formattedTemp.")
//    }
//  }
}