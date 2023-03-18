package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  /*: RDD[(List[String], List[(Int, String, List[String])])]*/
  def lookup(queries: RDD[List[String]]): RDD[(List[String], List[(Int, String, List[String])])] = {
    /*println("types to collect")*/
    var signature_calc = lshIndex.hash(queries)
    /*queries.foreach(println)
    signature_calc.foreach(println)*/
    var to_return = lshIndex.lookup(signature_calc)
      .map{case(a,b,c) =>(b,c)}
    if (to_return.isEmpty()){
      to_return = queries
        .map{case(a) => (a,Nil)}

    }
   /* println("what I return is")
    to_return.foreach(println)
    println("maaaaaamennnnneee")*/
    return to_return
    /*var sign = signature_calc.map(_._1).collect()
    println()
    sign.foreach(println)


    var looked_up = lshIndex.signature_computed
      .filter {
        case (_, _, _, ind) => sign.exists(_.sameElements(ind))
      }
      .groupBy(_._1)
      .flatMapValues(iterable => iterable.toList)
      .map { case (w, (attrib, id, title, ind)) => (w, (id, title, attrib)) }
      .groupByKey()
      .mapValues(_.toList)
      /*.map{case(a,(attribute,id,name,signa)) => (a)}*/
    looked_up.foreach(println)*/

    /*print("collected_movies")*/

  }
}
