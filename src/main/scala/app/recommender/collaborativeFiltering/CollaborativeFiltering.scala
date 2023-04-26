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

    /*rates = ratingsRDD.map{case (user, movie, old_rate, rate,time) =>
      Rating(user.toInt, movie.toInt, rate.toDouble)
    }


    /*println(rates.getClass)
    /*ratings.take(20).foreach(println)*/

    model = ALS.train(rates, rank, maxIterations, regularizationParameter,n_parallel,seed)

    usersProducts = rates.map { case Rating(user, movie, rate) =>
      (user, movie)
    }*/
    /*predictions.foreach(println)*/
    /*predictions.take(30).foreach(println)
    println(2)*/
    ratingsRDD.cache()*/

    // Convert the input RDD to Rating objects and cache the resulting RDD
    val rates = ratingsRDD.map { case (user, movie, old_rate, rate, time) =>
      Rating(user.toInt, movie.toInt, rate.toDouble)
    }.cache()

    // Partition the data and increase parallelism
    val numPartitions = 10
    val parallelism = 4
    val partitionedRates = rates.repartition(numPartitions)
    println("entering collab init")
    model = ALS.train(partitionedRates, rank, maxIterations, regularizationParameter, parallelism, seed)
    println("exiting collab init")
    // Extract the user-movie pairs for prediction
    usersProducts = rates.map { case Rating(user, movie, rate) =>
      (user, movie)
    }

    // Unpersist the cached RDDs to free up memory
    rates.unpersist()
    ratingsRDD.unpersist()
  }

  def predict(userId: Int, movieId: Int): Double = {

    var value_to_ret = model.predict(userId,movieId)

    return value_to_ret

  }

}
