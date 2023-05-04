package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import scala.reflect.internal.util.Statistics

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = new HashPartitioner(100)
  var movies_rating :  RDD[(Int, (Double, (Int, String, List[String])))] = _
  var movies_by_ID : RDD[(Int,(Int, String, List[String]))] = _
  var ratings_data: RDD[(Int, Int, Option[Double], Double, Int)] = _
  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    ratings_data = ratings
    movies_by_ID = title.groupBy(_._1).flatMapValues(iterable => iterable.toList).partitionBy(partitioner)

    var temp_movies_rating = ratings
      .groupBy(_._2)
      .flatMapValues(iterable => iterable.toList)
      .map{ case(a,(b,c,d,e,f)) => (a, e) }
      .groupBy(_._1)
      .mapValues(iterable => iterable.toList)
      .mapValues { values => values.map(_._2).sum / values.map(_._2).length}

    val array_rates = temp_movies_rating.collect()
    val col2Values = movies_by_ID
      .filter( x=> !array_rates.contains(x._2._1))

    movies_rating = temp_movies_rating.join(col2Values)

      /*.filter( x=> col2Values.contains(x._1))
      .join(movies_by_ID)*/



      /*.map(m => (m._2,m._1)).join(movies_by_ID.map(r => (r._1,r._2._2)))*/
    /*.mapValues{
        case Iter(a,b,c,d,e) => d
      }*/
    /*grouped_movies.foreach(println)*/


  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
     var return_result = movies_rating.map{case (a,(b,(c,d,e))) => (d,b)}
    return return_result
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    /*print(keywords)*/
    var reduce_rating = movies_rating
      .map{case (a,(b,(c,d,e))) => (d,b,e)}
    var filtered_movies = reduce_rating
      .filter {
        case (_, _, list) => keywords.forall(list.contains)
      }

    if (filtered_movies.count() == 0 ) return -1

    if (filtered_movies.foreach(_._2) == None) return 0
    /*filtered_movies.foreach(println)*/
    var result = filtered_movies
      .map(x => x._2)
    var return_value = (result.sum() / result.count())
    /*print(return_value)*/
    return return_value


  }


    /*movies_rating.foreach(println)*/
    /*filtered_movies.foreach(println)
    var a :Double = 2
    return a*/


  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {

    var update_data = delta_
    var updated_rating = ratings_data
      .map { case (a, b, c, d, e) =>
        update_data.find {
          m => m._1 == a && m._2 == b
        } match {
          case None =>
            (a, b, c, d, e)
          case Some((user_id, movie_id, old, new_rate, time)) =>

            /*update_data = update_data.filterNot { m =>
              m._1 == a && m._2 == b
            }*/
            /*update_data.foreach(println)*/
            (a, b, old, new_rate, e)

          case Some((user_id, movie_id, None, new_rate, time)) =>

            /*update_data = update_data.filterNot { m =>
              m._1 == a && m._2 == b
            }*/

            (a, b, c, new_rate, e)


        }
      }.collect()

    for ((a, b, _, _, _) <- updated_rating if update_data.exists(m => m._1 == a && m._2 == b)) {
      update_data = update_data.filterNot(m => m._1 == a && m._2 == b)
    }



    if (update_data.isEmpty) {
      ratings_data = sc.parallelize(updated_rating)
    }  else {
      var RDD_to_append = sc.parallelize(update_data)
      ratings_data = sc.parallelize(updated_rating) ++ RDD_to_append

    }
    movies_rating = movies_rating.unpersist()
    movies_rating = ratings_data
      .groupBy(_._2)
      .flatMapValues(iterable => iterable.toList)
      .map { case (a, (b, c, d, e, f)) => (a, e) }
      .groupBy(_._1)
      .mapValues(iterable => iterable.toList)
      .mapValues { values => values.map(_._2).sum / values.map(_._2).length }
      .join(movies_by_ID)
      .persist()

  }
}
