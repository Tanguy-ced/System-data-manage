package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {



  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratingsRDD[(Int, Int, Option[Double], Double, Int)]
   */
  def load():RDD[(Int, Int, Option[Double], Double, Int)]  = {
    val path_for_data = "src/main/resources"
    val file = path_for_data + path
    val new_rdd = sc.textFile(file)
    val rdd = new_rdd.map(_.replaceAll("\"", ""))
    val rdd_splited = {
      rdd.map(line => line.split("\\|"))
    }
    //rdd_splited.take(2).foreach { x =>
      //x.foreach(println)
    //}
    val old_rating: Option[Double] = None
    val rdd_return = rdd_splited.map( x => (x(0).toInt, x(1).toInt, old_rating , x(2).toDouble, x(3).toInt))

    //val rdd_return = rdd_splited.map {
      //case x if x.length == 5 => (x(0).toInt, x(1).toInt, x(2).toDouble , x(3).toDouble, x(4).toInt)
      //case x if x.length == 4 => (x(0).toInt, x(1).toInt, Option[Double]   , x(2).toDouble, x(3).toInt)

    //}
    return rdd_return
    //(x => (x(0).toInt, x(1).toInt, Option[Double]: Double = x.length match {case some(s) => x(2) case None => None} , x(3).toDouble, x(4).toInt)


  }
}