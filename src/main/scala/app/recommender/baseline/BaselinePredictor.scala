package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  /*private var state = null
  private var  movies_averaged :   RDD[(Int, Iterable[(Int, Option[Double], Double, Int)], Double)] = _
  private var movies_averaged_deviated : RDD[(Int, Iterable[(Int, Option[Double], Double, Int,Double)], Double)] = _
  private var movies_average_deviation :RDD[(Int, Double)] = _*/
  private var state = null
  private var movies_averaged: Map[Int, (Iterable[(Int, Option[Double], Double, Int)], Double)] = _
  private var movies_averaged_deviated : Map[Int, (Iterable[(Int, Option[Double], Double, Int,Double)], Double)] = _

  private var movies_average_deviation : Map[Int, Double] = _
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    /*println("exiting baseline init")
      var sorted_by_user = ratingsRDD
        .groupBy(_._1)
        .map { case (a, b) => (a, b, b.map(_._4).sum / b.size) }
      /*sorted_by_user.take(3).foreach(println)*/
      movies_averaged = sorted_by_user
        .map { case (user, iterable, average_overall) =>
          val updatedIterable = iterable.map {
            case (a, b, c, rate, e) =>
              val updated_rate = rate - average_overall
              (b, c, updated_rate, e)
          }
          (user, updatedIterable, average_overall)
        }
      /*movies_averaged.take(3).foreach(println)*/
      movies_averaged_deviated = movies_averaged
        .map { case (user, iterable, average_over) =>
          val update_deviation = iterable.map {
            case (movie_id, old, new_ave, time) =>
              val scale = new_ave match {
                case new_ave if new_ave > 0 => 5 - average_over
                case new_ave if new_ave < 0 => average_over - 1
                case new_ave if new_ave == 0 => 1
              }
              /*println(scale)*/
              val std = (new_ave / scale)
              (movie_id, old, new_ave, time, std)
          }
          (user, update_deviation, average_over)
        }
      /*movies_averaged_deviated.take(3).foreach(println)*/
      movies_average_deviation = movies_averaged_deviated
        .map { case (user, iterable, average_) => (user, iterable) }
        .flatMapValues(iterable => iterable.toList)
        .map { case (a, b) => b }
        .groupBy(_._1)
        .map { case (movie, iterable) =>
          val number_rating = iterable.size
          val overall_ave = (iterable.map(_._5).sum / number_rating)
    (movie, overall_ave)
  }
  /*movies_average_deviation.take(5).foreach(println)*/
  println("exiting baseline init")*/


    println("exiting baseline init")
    var sorted_by_user = ratingsRDD

      .groupBy(_._1)
      .map{ case (a, b) => (a, (b)) }
      .map { case (a, (b)) => (a, (b, b.map(_._4).sum / b.size)) }



      /*.toMap*/

    /*sorted_by_user.take(3).foreach(println)*/
     var movies_averaged = sorted_by_user
      .map { case (user, (iterable, average_overall)) =>
        val updatedIterable = iterable.map {
          case (a, b, c, rate, e) =>
            val updated_rate = rate - average_overall
            (b, c, updated_rate, e)
        }
        (user, (updatedIterable, average_overall))
      }


      /*.map { case (key, value, value2) =>
        key -> (value, value2)
      }*/
      /*.collect()
      .toSet
      .toMap{case (a,b)}*/
      /*.toMap(Int,(Iterable[(Int, Option[Double], Double, Int)], Double))*/
      /*.collect()*/
      /*.toMap*/
    /*movies_averaged.take(3).foreach(println)*/
    movies_averaged_deviated = movies_averaged
      .map { case (user,(iterable, average_over) ) =>
        val update_deviation = iterable.map {
          case (movie_id, old, new_ave, time) =>
            val scale = new_ave match {
              case new_ave if new_ave > 0 => 5 - average_over
              case new_ave if new_ave < 0 => average_over - 1
              case new_ave if new_ave == 0 => 1
            }
            /*println(scale)*/
            val std = (new_ave / scale)
            (movie_id, old, new_ave, time, std)
        }
        (user, (update_deviation, average_over))
      }
      .collectAsMap()
      .toMap


    /*movies_averaged_deviated.take(3).foreach(println)*/
    movies_average_deviation = movies_averaged_deviated
      .map { case (user, (iterable, average_)) => iterable }
      .flatten
      .groupBy(_._1)
      .map { case (movie, iterable) =>
        val number_rating = iterable.size
        val overall_ave = (iterable.map(_._5).sum / number_rating)
        (movie, overall_ave)
      }
      /*.map { case (a, b, c, d, e) =>*/
        /*(a, b, c, d, e * 2)*/ // example transformation
      /*.flatMapValues(iterable => iterable.toList)
      .map { case (a, b) => b }
      .groupBy(_._1)
      .map { case (movie, iterable) =>
        val number_rating = iterable.size
        val overall_ave = (iterable.map(_._5).sum / number_rating)
        (movie, overall_ave)
      }
      .collect()
      .toMap*/
    /*movies_average_deviation.take(5).foreach(println)*/
    println("exiting baseline init")

  }
      /*.flatMapValues(iterable => iterable.toList)*/

  def predict(userId: Int, movieId: Int): Double = {
    /*println("NEEEEEXT")*/
    /*var user_data = movies_averaged_deviated
      .filter{case (user_id,_,_)  => user_id == userId}
    var movie_data = movies_average_deviation
      .filter { case (movie_id, _) => movie_id == movieId }
    var average_user = user_data
      .map{case (user_I,iterable,average) => average}.collect()(0)
    var pop_std_mov = user_data
      .map{case (user_I,iterable,average) =>
        var std_movie = iterable.map{
          case (movie_id, old, new_ave, time ,std) => (std)
        }
        (std_movie).head
      }
    var movie_average_dev = movie_data
      .map(_._2)
      .collect()(0)


    var normalized = average_user + movie_average_dev

    var scaler = (normalized,average_user) match {
        case (normalized, average_user) if normalized > average_user => 5-average_user
        case (normalized, average_user) if normalized < average_user  => average_user-1
        case (normalized, average_user) if normalized == average_user => 1
      }

    var right_result = scaler * movie_average_dev
    /*println("right_res")
    println(right_result)
    println("right_res")
    println("average user : ")*/

    var result = average_user + right_result
    /*println(result)*/
    return result*/
    // Get user's average rating
    /*val average_user = movies_averaged_deviated
      .filter { case (user_id,( _, _)) => user_id == userId }
      .map { case (_,( _, average)) => average }
      .head*/
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
    /*val pop_std_mov =
    val pop_std_mov = movies_averaged_deviated.get(userId).map(_._1.flatMap(_._5)/*.flatMap(_._5).headOption).flatten.getOrElse(0.0)*/
*/
    // Normalize the deviation
    val normalized = average_user + movie_average_dev
    println(normalized)
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
