package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)
  var data_copy = data
  // Associate to each elements of the dataset the signature computed and stored in hash_table
  val hash_table : RDD[(IndexedSeq[Int], List[String])] = hash(data.map(_._3))
  val signature_computed: RDD[(List[String], Int, String ,IndexedSeq[Int])] = data_copy
    .map(m => (m._3,m)).join(hash_table.map(r=> (r._2,r._1)))
    .map{case (a,((b,c,d),e)) => (a,b,c,e)}
  // Filter the element appearing multiple time and transform into the expected structure
  var buckets : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])]=signature_computed
    .distinct()
    .groupBy(_._4)
    .flatMapValues(iterable => iterable.toList)
    .map { case (w, (attrib, id, title, ind)) => (w, (id, title, attrib)) }
    .groupByKey()
    .mapValues(_.toList)



  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    //Hashing function
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets():  RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    // Get the data structure with the signature
    return buckets
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)]):RDD[(IndexedSeq[Int],T, List[(Int, String, List[String])])] = {

    // Return elements matching the query

    var looked_up = buckets
      .join(queries)
      .map { case (key, (bucket, payload)) => (key,payload, bucket) }

    return looked_up


  }
}
