package xueting.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {
  
  /* function to extract (nameId -> name) or None in case of failure*/
  def parseNames(line: String) : Option[ (Int, String) ] = {
    // line: 10 "spider man"
    // spilt by " -> ['10 ', 'spider man']
    
    val fields = line.split("\"")
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))  // trim() to get rid of spaces in front and end.
    } else {
      return None // flatMap will just discard 'None' results, and extract data from 'Some' results.
    }   
  }
  
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")
    
    // build up RDD (nameID -> name) tuples
    val names = sc.textFile("data/marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // Load up superhero co-occurrence data
    val lines = sc.textFile("data/marvel-graph.txt")
    // get (heroName, # of occurrences (i.e., length - 1))
    // split by any space (like tab,...)
    val heroFriends = lines.map( x => { val y = x.split("\\s+"); (y(0).toInt, y.length - 1) } )
    
    val hearoTotalFriends = heroFriends.reduceByKey( (x, y) => x + y )
    val flipped = hearoTotalFriends.map( x => (x._2, x._1) )
    
    val mostPopular = flipped.max()

    // look up the hero name
    // lookup returns an array of results, so we need to access the first result with (0)).
    
    val name = namesRdd.lookup(mostPopular._2)(0)
    println(s"$name is the most popular superhero with ${mostPopular._1} co-appearances")
    
    /* Follow-up: print top 10 popular superheroes. 
     * 1. sorted by (# of co-occurrences)
     * 2. take top 10
     * 3. for each, lookup the name from namesRdd
     * */
    val descPopulars = flipped.sortByKey(false)
    val most10 = descPopulars.take(10)
    for (pair <- most10) {
      val name = namesRdd.lookup(pair._2)(0)
      println(s"$name has ${pair._1} co-appearances.")
      
    }
  }
  
 
}
