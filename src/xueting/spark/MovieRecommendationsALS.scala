package xueting.spark

import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.execution.datasources.text.TextFileFormat


/*
 * 1. before run, we need to add a fake person with userID = 0 in "u.data" for our predicting process
 * 
 * 
 * When running the MovieRecommendationsALS.scala script in the next lecture, 
 * sometimes you'll encounter very cryptic errors about object serialization 
 * when running it within Eclipse (or IntelliJ.) 
 * 
 * If this happens to you, follow these steps to run it using spark-submit instead:
 * Right-click the MovieRecommendationsALS.scala script in the package explorer, 
 * and select Export...Choose Java / JAR file and hit Next.
 * 
 * Set the export destination to C:\SparkScala\SparkScalaCourse\als.jar 
 * (substitute your own workspace directory if it's different.)
 * 
 * Hit Finish
 * Open up a command prompt
 * Enter cd c:\SparkScala\SparkScalaCourse (substitute your own workspace directory if it's different)
 * Enter spark-submit --class com.sundogsoftware.spark.MovieRecommendationsALS als.jar 0
 * Now it should run, apart from some errors at shutdown that are safe to ignore.
 * 
 * This seems to be happening more often with spark-3.0.0-preview, 
 * so I'm hoping the underlying issue will be resolved in a future Spark release.
 * */


object MovieRecommendationsALS {
   
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("data/ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')  // use ' not ", otherwise, key error will cause when pair movieId and movieName
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  // Row format to feed into ALS
  case class Rating(userId: Int, movieId: Int, rating: Float)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Make a session
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    
    println("Loading movie names...")
    val nameDict = loadMovieNames()
 
    val data = spark.read.textFile("data/ml-100k/u.data")
    
    val ratings = data.map( x => x.split('\t') ).map( x => Rating(x(0).toInt, x(1).toInt, x(2).toFloat) ).toDF()
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    
    val model = als.fit(ratings)
      
    // Get top-10 recommendations for the user we specified
    val userID:Int = args(0).toInt
    val users = Seq(userID).toDF("userId")
    val recommendations = model.recommendForUserSubset(users, 10)
    
    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")
 
    for (userRecs <- recommendations) {
      val myRecs = userRecs(1) // First column is userID, second is the recs
      val temp = myRecs.asInstanceOf[WrappedArray[Row]] // Tell Scala what it is
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = nameDict(movie)
        println(movieName, rating)
      }
    }
    
    // Stop the session
    spark.stop()

  }
}