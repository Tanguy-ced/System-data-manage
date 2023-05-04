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
    val load_file = sc.textFile(file)
    val replace_sep = load_file.map(_.replaceAll("\"", ""))
    val rdd_splited = {
      replace_sep.map(line => line.split("\\|"))
    }
    /* Here I convert the string to the desired format*/
    val movies_loaded : RDD[( Int, String, List[String])] = rdd_splited.map(x => (x(0).toInt, x(1), x.slice(2 , x.length).toList))

    return movies_loaded

  }
}

