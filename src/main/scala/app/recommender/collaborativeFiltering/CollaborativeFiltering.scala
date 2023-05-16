package app.recommender.collaborativeFiltering
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var rates : RDD[Rating] = _
  private var model : MatrixFactorizationModel = _
  var usersProducts : RDD[(Int,Int)] = _
  private var predictions : RDD[((Int,Int),Double)] = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    val rates = ratingsRDD.map { case (user, movie, old_rate, rate, time) =>
      Rating(user.toInt, movie.toInt, rate.toDouble)
    }.cache()

    // Train the model
    model = ALS.train(rates, rank, maxIterations, regularizationParameter, n_parallel, seed)
    // Extract the user-movie pairs for prediction
    usersProducts = rates.map { case Rating(user, movie, rate) =>
      (user, movie)
    }

  }

  def predict(userId: Int, movieId: Int): Double = {

    var value_to_ret = model.predict(userId,movieId)

    return value_to_ret

  }

}
