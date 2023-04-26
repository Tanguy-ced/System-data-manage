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
  sc.setCheckpointDir("target")


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
    val rdd_return = rdd_splited.map( x => (x(0).toInt, x(1).toInt, None.asInstanceOf[Option[Double]] , x(2).toDouble, x(3).toInt))

    return rdd_return


  }
}