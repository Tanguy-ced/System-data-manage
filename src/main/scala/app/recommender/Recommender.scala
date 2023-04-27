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


    val user_movie = res.flatMap(x => {
      val list = x._2
      for (elem <- list) yield (userId, elem._1)
    })
      .subtract(collaborativePredictor.usersProducts)

    var user_movie_arr = user_movie.collect()
    println("joined")
    /*user_movie_arr.foreach(println)*/
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
    /*val to_return_maggle = List((1, 1.5), (2, 2.5), (3, 3.5))
  */
    return to_return_maggle
  }



  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    var query = sc.parallelize(List(genre))
      /*.map(line => line.split(",").toList)
      .flatMap(x=>x.flatMap(y => y))
      .collect()
      .toList*/
    /*query.foreach(println)*/
    val res = nn_lookup
      .lookup(query)
      /*.filter(x => x._2.filter(y =>
        x._1.size == y._3.size && x._1.zip(y._3).filter(a => a._1 != a._2).size == 0
      ).size != 0)*/

    val user_movie = res.flatMap(x => {
      val list = x._2
      for (elem <- list) yield (userId, elem._1)
    })
      .subtract(collaborativePredictor.usersProducts)

    var user_movie_arr = user_movie.collect()
    println("joined")
    /*user_movie_arr.foreach(println)*/
    var id_movie = user_movie.map(x => x._2)
    var rate = Array.empty[(Int,Double)]
    for (elem <- user_movie_arr) {
      rate:+= (elem._2,collaborativePredictor.predict(elem._1,elem._2))
    }
    /*rate.foreach(println)*/
    val rate_rdd = sc.parallelize(rate)
    /*rate_rdd.foreach(println)*/
      /*.repartition(id_movie.getNumPartitions)
    val numPartitions = math.max(rate_rdd.getNumPartitions, id_movie.getNumPartitions)
    val repartitionedRDD1 = rate_rdd.repartition(numPartitions)
    val repartitionedRDD2 = id_movie.repartition(numPartitions)
    println("rates : ")
    repartitionedRDD1.foreach(println)
    println("user_id : ")
    repartitionedRDD2.foreach(println)
    println(repartitionedRDD1.getNumPartitions)
    println(repartitionedRDD2.getNumPartitions)*/
    val to_return_maggle = rate_rdd
      .sortBy(-_._2)
      .take(K)
      .toList
    /*val to_return_maggle = List((1, 1.5), (2, 2.5), (3, 3.5))
*/
    return to_return_maggle
    /*movie_to_guess.foreach{elem => elem._2 = collaborativePredictor.predict(elem._1,elem._2)}
*/
    /*val Calculate_rate = joined
      .foreach( x => {x._2 = collaborativePredictor.predict(x._1,x._2)}  )

    Calculate_rate.foreach(println)*/

    /*val movie_iden = index.data_copy
          .map{ case (movie_id, name_movie, _) => (name_movie,movie_id) }
        println("movie_iden")
        movie_iden.foreach(println)
        val joined = remove_type.join(movie_iden)
          .map { case (name, (user_id, movie_id)) => (userId, movie_id) }
        println("joined")
        joined.foreach(println)*/
    /*for ((user, name) <- remove_type; (id, name, type_) <- movie_id) yield (user, id))*/
    /*val movie_id = remove_type*/
    /*println(res.count())
    println(moviesLoader.load().count())
    assert(res.count() == moviesLoader.load().count())*/

      /*.lookup(genre)
      .collect()
*/
  }
}
