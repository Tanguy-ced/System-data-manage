package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {


  private var state = null
  private var movies_averaged: Map[Int, (Iterable[(Int, Option[Double], Double, Int)], Double)] = _
  private var movies_averaged_deviated : Map[Int, (Iterable[(Int, Option[Double], Double, Int,Double)], Double)] = _

  private var movies_average_deviation : Map[Int, Double] = _
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Map to a structure of the type [user_id , ]
    var sorted_by_user = ratingsRDD
      .groupBy(_._1)
      .map{ case (a, b) => (a, (b)) }
      .map { case (a, (b)) => (a, (b, b.map(_._4).sum / b.size)) }

     var movies_averaged = sorted_by_user
      .map { case (user, (iterable, average_overall)) =>
        val updatedIterable = iterable.map {
          case (a, b, c, rate, e) =>
            val updated_rate = rate - average_overall
            (b, c, updated_rate, e)
        }
        (user, (updatedIterable, average_overall))
      }

    movies_averaged_deviated = movies_averaged
      .map { case (user,(iterable, average_over) ) =>
        val update_deviation = iterable.map {
          case (movie_id, old, new_ave, time) =>
            val scale = new_ave match {
              case new_ave if new_ave > 0 => 5 - average_over
              case new_ave if new_ave < 0 => average_over - 1
              case new_ave if new_ave == 0 => 1
            }
            val std = (new_ave / scale)
            (movie_id, old, new_ave, time, std)
        }
        (user, (update_deviation, average_over))
      }
      .collectAsMap()
      .toMap

    movies_average_deviation = movies_averaged_deviated
      .map { case (user, (iterable, average_)) => iterable }
      .flatten
      .groupBy(_._1)
      .map { case (movie, iterable) =>
        val number_rating = iterable.size
        val overall_ave = (iterable.map(_._5).sum / number_rating)
        (movie, overall_ave)
      }


  }
      /*.flatMapValues(iterable => iterable.toList)*/

  def predict(userId: Int, movieId: Int): Double = {

    val average_user = movies_averaged_deviated.get(userId).map(_._2).getOrElse(0.0)
    // Get movie's average deviation
    val movie_average_dev = movies_average_deviation
      .filter { case (movie_id, _) => movie_id == movieId }
      .map { case (_, dev) => dev }
      .head

    // Get standard deviation of movie ratings for user
    val pop_std_mov = movies_averaged_deviated
      .filter { case (user_id, (_, _) )=> user_id == userId }
      .flatMap { case (_, (iterable, _))=> iterable.map { case (_, _, _, _, std) => std } }
      .head

    // Normalize the deviation
    val normalized = average_user + movie_average_dev
    // Calculate the scaler
    val scaler = math.signum(normalized - average_user) match {
      case 1 => 5 - average_user
      case -1 => average_user - 1
      case _ => 1
    }

    // Calculate the final result
    val result = average_user + scaler * movie_average_dev

    return result

  }
}
