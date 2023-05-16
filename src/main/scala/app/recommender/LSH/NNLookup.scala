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

    // compute the signature of the queries
    var signature_calc = lshIndex.hash(queries)

    // Use the lookup fonction to get the grouping according to the query
    var to_return = lshIndex.lookup(signature_calc)
      .map{case(a,b,c) =>(b,c)}
    if (to_return.isEmpty()){
      to_return = queries
        .map{case(a) => (a,Nil)}
    }
    return to_return
  }
}

