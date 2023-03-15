package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  var movies_by_ID : RDD[(Int,(Int, String, List[String]))] = _
  var ratings_ : RDD[(Int, (Int, (Int,Int, Option[Double], Double, Int)))] = _
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    movies_by_ID = movie.groupBy(_._1).flatMapValues(iterable => iterable.toList)

  /*  movies_by_ID.foreach(println)*/
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

    //println(ratings_.getClass)
  }



  def getNumberOfMoviesRatedEachYear:RDD[(Int, Int)] = {
    //ratings_mag.foreach(println)
    val reduce = ratings_.map { case (a, (b, (c, d, e, f, g))) => (a, b) }
    reduce.take(20).foreach(println)
    val result = reduce
      .distinct()
      .groupBy(_._2)

      .map{case (w, l) => (w, l.size)}
    result.foreach(println)
    return result


  }

  def getMostRatedMovieEachYear: RDD[(Int, String)]={

    /*ratings_.foreach(println)*/
    val reduce = ratings_.map {case(a,(b,(c,d,e,f,g))) => (a,b)}
    /*reduce.take(2).foreach(println)*/
    val result = reduce
      .groupBy(_._2)
      .map{case (w, l) => (w,l.groupBy(identity).maxBy(g => (g._2.size,g._1._1))._1._1)
      }

    val joined_movie = result.map(m => (m._2,m._1)).join(movies_by_ID.map(r => (r._1,r._2._2)))

    val result_ret = joined_movie.map{case (key,(year,movie_name)) => (year,movie_name)}
    result_ret.take(20)foreach println
    return result_ret

  }



  def getMostRatedGenreEachYear: RDD[(Int, List[String])] =
    {
      val reduce = ratings_.map { case (a, (b, (c, d, e, f, g))) => (a, b) }
      /*reduce.take(2).foreach(println)*/
      val result = reduce
        .groupBy(_._2)
        .map{case (w, l) => (w,l.groupBy(identity).maxBy(g => (g._2.size,g._1._1))._1._1)


        }

      val joined_movie = result.map(m => (m._2,m._1)).join(movies_by_ID.map(r => (r._1,r._2._3)))
      /*joined_movie.foreach(println)*/
      val result_genre = joined_movie.map { case (key, (year, movies_genre)) => (year, movies_genre) }
      /*result_genre.take(20) foreach println*/
      return result_genre
    }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String,Int),(String,Int)) = {

      val reduce = ratings_.map { case (a, (b, (c, d, e, f, g))) => (a, b) }
      reduce.take(2).foreach(println)
      val result = reduce
        .groupBy(_._2.unary_+)
        .map { case (w, l) => (w, l.groupBy(identity).maxBy(g => (g._2.size, g._1._1))._1._1)


        }

      val joined_movie = result.map(m => (m._2, m._1)).join(movies_by_ID.map(r => (r._1, r._2._3)))
      /*joined_movie.foreach(println)*/
      val result_genre = joined_movie.map { case (key, (year, movies_genre)) => movies_genre }
     /* result_genre.take(20) foreach println*/
      val grouped_genre = result_genre
        .flatMap(x => x)
        .groupBy(x=>x)
        .mapValues(_.size)
      val max_genre = grouped_genre.collect().maxBy(g => (g._2,g._1))
      val min_genre = grouped_genre.collect().minBy(g => (g._2,g._1))
      val  min_and_most = (min_genre,max_genre)
      print(min_and_most)
      return min_and_most

  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    /*movies.foreach(println)
    requiredGenres.foreach(println)*/
    val requiredGenresList = requiredGenres.collect().toList
    val filtered_genre = movies.filter(_._3.exists(requiredGenresList.contains))
    filtered_genre.foreach(println)
    val take_movie = filtered_genre
      .map{case (a,b,c) => b}
    take_movie.foreach(println)

    return take_movie
  }

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
                                         broadcastCallback: List[String] => Broadcast[List[String]]) :RDD[String] = {
    val requiredGenresBroad = broadcastCallback(requiredGenres)

    val filtered_genre = movies.filter(_._3.exists(requiredGenresBroad.value.contains))
    filtered_genre.foreach(println)
    val take_movie = filtered_genre
      .map { case (a, b, c) => b }
    take_movie.foreach(println)

    return take_movie



  }

}

