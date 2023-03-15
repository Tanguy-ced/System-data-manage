package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

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
    movies_by_ID = title.groupBy(_._1).flatMapValues(iterable => iterable.toList).partitionBy(partitioner)

    movies_rating = ratings
      .groupBy(_._2)
      .flatMapValues(iterable => iterable.toList)
      .map{ case(a,(b,c,d,e,f)) => (a, e) }
      .groupBy(_._1)
      .mapValues(iterable => iterable.toList)
      .mapValues { values => values.map(_._2).sum / values.map(_._2).length}
      .join(movies_by_ID)


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
  def getKeywordQueryResult(keywords: List[String]): Double = ???

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = ???
}
