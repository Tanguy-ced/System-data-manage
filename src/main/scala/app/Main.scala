package app

import app._
import app.loaders.MoviesLoader
import app.loaders.RatingsLoader
import org.apache.spark.{SparkConf, SparkContext}
import app.aggregator.Aggregator
import app.analytics.SimpleAnalytics


object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir("target")
    val Movies = new MoviesLoader(sc, "/movies_small.csv");
    val my_movies = Movies
      .load()
      //.collect()
    //my_movies.foreach(println)
    //my_movies = sc.parallelize(my_movies)

    val Ratings = new RatingsLoader(sc, "/ratings_small.csv")
    val my_ratings = Ratings
      .load()
      //.collect()
    //my_ratings.take(10).foreach(println)
    //my_ratings = sc.parallelize(my_movies)
    //your code goes here
    val analytics = new SimpleAnalytics()
    analytics.init(my_ratings,my_movies)
    val nb_movies = analytics.getNumberOfMoviesRatedEachYear
   /* nb_movies.foreach(println)*/

    //val a = 5

    //val res = analytics.getNumberOfMoviesRatedEachYear
      //.collect()


  }
}
