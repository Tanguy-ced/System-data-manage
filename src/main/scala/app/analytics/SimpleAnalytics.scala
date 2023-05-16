package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = new HashPartitioner(100)
  private var moviesPartitioner: HashPartitioner = new HashPartitioner(100)
  private var movies_by_ID : RDD[(Int,(Int, String, List[String]))] = _
  private var ratings_ : RDD[(Int, (Int, (Int,Int, Option[Double], Double, Int)))] = _
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    movies_by_ID = movie
      .groupBy(_._1) // group by ID
      .flatMapValues(iterable => iterable.toList) // convert iterable to List
      .partitionBy(moviesPartitioner)
      .persist()

    // Group by year and then by movie ID
    var ratings_temp = ratings.map { case (a, b, c, d, e) =>
      val dt = new DateTime(e * 1000L) // convert seconds to milliseconds
      (a, b, c, d, dt.getYear) // extract the year
    }
      .groupBy(_._5)
      .flatMapValues(iterable => iterable.toList)
      .groupBy(_._2._2)
      .flatMapValues(iterable => iterable.toList)

    //Partition the ratings
    ratings_ =  ratings_temp.partitionBy(ratingsPartitioner).persist()

  }

  def getNumberOfMoviesRatedEachYear:RDD[(Int, Int)] = {

    // Map to movie ID and Year
    val reduce = ratings_.map { case (a, (b, (c, d, e, f, g))) => (a, b) }

    // Extract the number of movies rated each year by grouping by year and extracting the size
    val result = reduce
      .distinct()
      .groupBy(_._2)
      .map{case (w, l) => (w, l.size)}

    return result


  }

  def getMostRatedMovieEachYear: RDD[(Int, String)]={

    // Map to movie ID and Year
    val reduce = ratings_.map {case(a,(b,(c,d,e,f,g))) => (a,b)}

    // Group by year, calculate the frequency of appearence of each movie and select the maximum
    val result = reduce
      .groupBy(_._2)
      .map{case (w, l) => (w,l.groupBy(identity).maxBy(g => (g._2.size,g._1._1))._1._1)
      }

    // Map from [ID_user , ID_movie] to [ID_user , Name_movie]
    val joined_movie = result.map(m => (m._2,m._1)).join(movies_by_ID.map(r => (r._1,r._2._2)))

    val result_ret = joined_movie.map{case (key,(year,movie_name)) => (year,movie_name)}

    return result_ret

  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] =
  {
    // The same process as before but we return the list of genre instead of the name of the movie
    val reduce = ratings_.map { case (a, (b, (c, d, e, f, g))) => (a, b) }


    val result = reduce
      .groupBy(_._2)
      .map{case (w, l) => (w,l.groupBy(identity).maxBy(g => (g._2.size,g._1._1))._1._1)
      }

    val joined_movie = result.map(m => (m._2,m._1)).join(movies_by_ID.map(r => (r._1,r._2._3)))

    val result_genre = joined_movie.map { case (key, (year, movies_genre)) => (year, movies_genre) }

    return result_genre
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String,Int),(String,Int)) =
  {
    // Same Process as before
    val reduce = ratings_.map { case (a, (b, (c, d, e, f, g))) => (a, b) }
    val result = reduce
      .groupBy(_._2.unary_+)
      .map { case (w, l) => (w, l.groupBy(identity).maxBy(g => (g._2.size, g._1._1))._1._1)


      }

    val joined_movie = result.map(m => (m._2, m._1)).join(movies_by_ID.map(r => (r._1, r._2._3)))

    val result_genre = joined_movie.map { case (key, (year, movies_genre)) => movies_genre }

    // Calculate the frequency of appearence of each genre
    val grouped_genre = result_genre
      .flatMap(x => x)
      .groupBy(x=>x)
      .mapValues(_.size)

    // Extract the maximum and the minimum occurence
    val max_genre = grouped_genre.collect().maxBy(g => (g._2,g._1))
    val min_genre = grouped_genre.collect().minBy(g => (g._2,g._1))
    val  min_and_most = (min_genre,max_genre)

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


    val requiredGenresList = requiredGenres.collect().toList

    // Select the movies that have the required genres
    val filtered_genre = movies.filter(_._3.exists(requiredGenresList.contains))

    val take_movie = filtered_genre
      .map{case (a,b,c) => b}

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

    // Same as before but by using broadcast

    val requiredGenresBroad = broadcastCallback(requiredGenres)
    val filtered_genre = movies.filter(_._3.exists(requiredGenresBroad.value.contains))
    val take_movie = filtered_genre
      .map { case (a, b, c) => b }

    return take_movie
  }

}

