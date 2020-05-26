package xueting.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction

/** Find the movies with the most ratings. */
object PopularMovies {
  
  /* Load up a map of movieID -> movieName */
  def loadMovieNames(): Map[Int, String] = {

    // handle character encoding issue
    implicit val codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.
    
    // create a map of Int to Strings, and populate it from u.item
    var movieNames:Map[Int, String] = Map()
    
    val lines = Source.fromFile("data/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|') // use "|" will cause 'key not found' error. NO idea why
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }
  
 
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "PopularMovies")
    
    // create a broadcast variable of our MovieID -> movieName map
    var nameDict = sc.broadcast(loadMovieNames)
    
    val lines = sc.textFile("data/ml-100k/u.data")
    
    val movies = lines.map( x => (x.split("\t")(1).toInt, 1) )
    
    val moviesCount = movies.reduceByKey( (x, y) => x + y )
    
    val moviesCountWithName = moviesCount.map( x => (nameDict.value(x._1), x._2) )
    
    val results = moviesCountWithName.collect()
    
    results.sortBy(_._2).foreach(println)
    
    
    
  }
  
//      // Create a broadcast variable of our ID -> movie name map
//    var nameDict = sc.broadcast(loadMovieNames)

//    // Fold in the movie names from the broadcast variable
//    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
//    
//    // Collect and print results
//    val results = sortedMoviesWithNames.collect()
//    
//    results.foreach(println)
}

