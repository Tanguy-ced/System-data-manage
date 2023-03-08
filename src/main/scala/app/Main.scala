package app

import app._
import app.loaders.MoviesLoader
import app.loaders.RatingsLoader
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    val Movies = new MoviesLoader(sc, "src/main/resources/movies_small.csv");
    val my_movies = Movies
      .load()
      .collect()
      .sortWith((t1, t2) => t1.toString() <= t2.toString())
    my_movies.take(2).foreach(println)
    val Ratings = new RatingsLoader(sc, "src/main/resources/ratings_small.csv")
    val my_ratings = Ratings.load()
    //your code goes here
  }
}
