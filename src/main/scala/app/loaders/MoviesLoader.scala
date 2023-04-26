package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.spark_project.dmg.pmml.Characteristic
import shapeless.Tuple

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles RDD[(Int, String, List[String])] =
   */
  def load(): RDD[( Int, String, List[String])]   ={
    val path_for_data = "src/main/resources"
    val file = path_for_data + path
    val new_rdd = sc.textFile(file)
    val rdd = new_rdd.map(_.replaceAll("\"", ""))
    val rdd_splited = {
      rdd.map(line => line.split("\\|"))
    }

    val rdd_return : RDD[( Int, String, List[String])] = rdd_splited.map(x => (x(0).toInt, x(1), x.slice(2 , x.length).toList))
    return rdd_return

  }
}

