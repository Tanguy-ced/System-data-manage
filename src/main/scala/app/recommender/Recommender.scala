package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    var query = sc.parallelize(List(genre))


    val res = nn_lookup
      .lookup(query)

    //
    val user_movie = res.flatMap(x => {
      val list = x._2
      for (elem <- list) yield (userId, elem._1)
    })
      .subtract(collaborativePredictor.usersProducts)

    var user_movie_arr = user_movie.collect()

    var id_movie = user_movie.map(x => x._2)
    var rate = Array.empty[(Int, Double)]
    for (elem <- user_movie_arr) {
      rate :+= (elem._2, baselinePredictor.predict(elem._1, elem._2))
    }
    val rate_rdd = sc.parallelize(rate)

    val to_return_maggle = rate_rdd
      .sortBy(-_._2)
      .take(K)
      .toList

    return to_return_maggle
  }



  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    var query = sc.parallelize(List(genre))

    val res = nn_lookup
      .lookup(query)

    val user_movie = res.flatMap(x => {
      val list = x._2
      for (elem <- list) yield (userId, elem._1)
    })
      .subtract(collaborativePredictor.usersProducts)

    var user_movie_arr = user_movie.collect()
    var id_movie = user_movie.map(x => x._2)
    var rate = Array.empty[(Int,Double)]
    for (elem <- user_movie_arr) {
      rate:+= (elem._2,collaborativePredictor.predict(elem._1,elem._2))
    }

    val rate_rdd = sc.parallelize(rate)
    val to_return_maggle = rate_rdd
      .sortBy(-_._2)
      .take(K)
      .toList
    return to_return_maggle
  }
}
