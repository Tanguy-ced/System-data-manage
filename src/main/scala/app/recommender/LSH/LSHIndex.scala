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
  val yallah : RDD[(IndexedSeq[Int], List[String])] = hash(data.map(_._3))
  val signature_computed: RDD[(List[String], Int, String ,IndexedSeq[Int])] = data_copy
    .map(m => (m._3,m)).join(yallah.map(r=> (r._2,r._1)))
    .map{case (a,((b,c,d),e)) => (a,b,c,e)}
  var buckets : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])]=signature_computed
    .distinct()
    .groupBy(_._4)
    .flatMapValues(iterable => iterable.toList)
    .map { case (w, (attrib, id, title, ind)) => (w, (id, title, attrib)) }
    .groupByKey()
    .mapValues(_.toList)


  /*buckets.foreach(println)*/
  /*.map(m => (m._2,m._1)).join(movies_by_ID.map(r => (r._1,r._2._2)))*/
  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets():  RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {

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

   /* println("the first key is ")*/
    /*queries.foreach(println)*/
    /*println("make space for the buckets ")*/
    /*buckets.foreach(println)*/
    /*println("space made ")*/
    var looked_up = buckets
      .join(queries)
      .map { case (key, (bucket, payload)) => (key,payload, bucket) }

    /*queries
      .join(buckets)
      /*.map { case (key, ( payload, bucket)) => (key,payload, bucket) }*/*/
      /*.distinct()*/

    /*println("selected movies")*/
    /*looked_up.foreach(println)
    println("Pas mal bg")*/
      /*.map{case (a)}*/
      /*.groupBy(_._1)
      .flatMapValues(iterable => iterable.toList)
      .map { case (w, (attrib, id, title, ind)) => (w, (id, title, attrib)) }
      .groupByKey()
      .mapValues(_.toList)*/
    /*.map{case(a,(attribute,id,name,signa)) => (a)}*/

    /*return looked_up*/
    return looked_up
    /*print("collected_movies")*/


  }
}
/*RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])]*/