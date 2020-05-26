# Apache Spark with Scala

17 hands-on examples for studying Apache Spark with Scala of analyzing large data sets on my local machine and Amazon EMR.
1. Learn basic operations for RDD.
    - [Purchase By Customer using map()](#purchasebycustomer)
    - [Ratings counter using countBy()](#ratingscounter)
    - [Word counter better sorted using regular expression](#wordcountbettersorted)
    - [Average friends counter by age using map() and reduce()](#friendsbyage)
    - [Most popular superhero using flatMap()](#mostpopularsuperhero)
    - [Find popular movies on RDD](#popularmovies)
    - [Find similar movies using filter() and cache()](#moviesimilarities)
   
2. Learn basic operations for datsets.
    - [Find popular movies on dataset](#popularmoviesdatasets)
    - [Learn DataFrame](#dataframes)
    - [Min Temperatures by station using RDD or datset](#mintemperatures)
    - [Learn Spark SQL](#sparksql)
    
3. Apply BFS to find degree of separation
    - [BFS to find degree of separation based on co-occurrences without GraphX](#degreesofseparation)
    - [BFS with GraphX and Pregel API](#graphx)

4. Spark Machine Learning Library:
    - [Linear regression using dataframes](#linearregressiondataframe)
    - [Movie Recommendations using ALS algorithm](#movierecommendsals)
    - [1 million movie Recommendations run on Amazon EMR](#moviesimilarities1m)

5. Spark Streaming:
    - [Find most popular Twitter hashtags in past 5 mintues](#popularhashtags)

### PurchaseByCustomer
Count the total amount spent for each customer.
Learn Spark map() function.

### RatingsCounter
Count up how many of each star rating exists in the MovieLens 100K data set.
Learn countBy() action function.

### WordCountBetterSorted
Count up how many of each word occurs in a book, using regular expressions and sorting the final results.

### FriendsByAge
Compute the average number of friends by age in a social network.

Learn RDD, map function, and reduce function.

### MostPopularSuperhero
Find the superhero with the most co-appearances.

Learn flatMap() and lookup() for RDD's.

### PopularMovies
Find the movies with the most ratings using RDD.

### PopularMoviesDataSets
Find the movies with the most ratings using Datasets.

### DataFrames
Learn Dataset and DataFrame in Spark.

### MinTemperatures
Find the minimum temperatures by station.

Learn difference between RDD and datasets.

1. Use filter(), map(), and reduce() function using RDD.
2. Use filter() and groupBY() functions using Datasets.

### DegreesOfSeparation

Find the degrees of separation between two Marvel comic book characters, 
based on co-appearances in a comic.

Use the Breadth-First-Search algorithm to traverse the social network 
and find the separation between Spider Man and ADAM.

### MovieSimilarities
Find top 10 similar movies for the movie user specified using cosine similarity.

Learn RDD's filter() and cache() functions.

Cache RDD for later actions to avoid reconstructing RDD.


### SparkSQL
Learn how to convert a structured RDD into a dataset.

Learn how to run SQL queries in Spark over datasets.


### LinearRegressionDataFrame
Learn Spark ML library with linear regression of DataFrame instead of RDD.

Try to find the linear relationship of page speed and amount spent.

### MovieRecommendsALS
Build the movies recommendation model on DataFrames using Alternating Least Squares in Spark ML library.

Get top-10 recommendations for the user we specified.

### MovieSimilarities1M
For 1 million movies, find top 50 similar movies for the movie user specified using cosine similarity.

Learn how to run Spark codes on a real cluster (Amazon EMR) using data from AWS S3.

Learn 'spark-submit' command to run Spark codes through terminal.

### PopularHashtags
Listens to a stream of Tweets and keeps track of the most popular hashtags over a 5 minutes window.

Learn Spark dataset streaming and reduceByKeyAndWindow() function.

Learn how to configure Twitter service credentials.

### GraphX
Learn GraphX in action with the Marvel superhero dataset.

Do Breadth-First Search using the Pregel API.

Find the top 10 most-connected superheroes, using graph.degrees.

Find the degrees from SpiderMan to ADAM 3,031.







