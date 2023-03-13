package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  var moves : RDD[(Int, String, List[String])] = _
  var ratings_ : RDD[(Int, (Int, (Int,Int, Option[Double], Double, Int)))] = _
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    moves = movie
    moves.groupBy(_._1)
    //moves.foreach(println)
    var ratings_temp = ratings.map { case (a, b, c, d, e) =>
      (a, b, c, d, (e / 31557600) + 1970)
    }
      .groupBy(_._5)
      .flatMapValues(iterable => iterable.toList)
      .groupBy(_._2._2)
      .flatMapValues(iterable => iterable.toList)

    //ratings_temp.take(100)foreach(println)
    ratings_ =  ratings_temp
    //ratings_.foreach(println)

    //ratings_mag.foreach(println)

      //.persist()

      //.partitionBy(ratingsPartitioner)

    println(ratings_.getClass)
  }



  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    //ratings_mag.foreach(println)
    var pipi : RDD[(Int, Int)] = ratings_
      .groupBy(_._2._1)
      .mapValues(_.size)
      .sortByKey()
    //pipi.foreach(print)

    return pipi
  }

  def getMostRatedMovieEachYear: Unit ={

    ratings_.foreach(println)
    val reduce = ratings_.map {case(a,(b,(c,d,e,f,g))) => (a,b)}
    reduce.take(2).foreach(println)
    val result = reduce
      .groupBy(_._2)
      .mapValues(_.map(_._1))
      .map{case (w, l) =>
        val mostFrequent = l.groupBy(identity).maxBy(_._2.size)._1
        List(w,mostFrequent)

      }










    result.foreach(println)
    //println(ratings_.count(_.1))
    /*var sorted = bang
      .groupBy(_._2.)



      sorted.foreach(println)*/
      /*.mapValues(_._2)
      */




    /*var find_most = bang
      .groupBy(_)*/

      //.sortByKey()
    //bang.foreach(print)
  }



  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = ???

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = ???

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = ???

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = ???

}

