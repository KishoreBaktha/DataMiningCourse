import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.util.Random
import scala.collection.mutable.{ListBuffer, Map => MutableMap}

object Similarity {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Similarity Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val docs = sc.wholeTextFiles("data/", 2).cache()
      .map({ case (path, content) => (path.split("/").last, content) })
      .mapValues(_.replace("\r\n", " "))

    val k = 10
    var counter1 = 0
    var counter2 = 0
    val shingles = docs
      .mapValues(createShingles(_, k))
      .mapValues(hashShingles)

    println("Using Hashed Shingles")

    val shinglesMap = shingles.collectAsMap()
    var results = ListBuffer[(String, Double)]()

    var time1 = System.currentTimeMillis()
    for ((k, v) <- shinglesMap) {
      counter1 = counter1 + 1
      for ((k2, v2) <- shinglesMap) {
        if (counter2 >= counter1) {
          results += ((s"$k, $k2", compareSets(v, v2)))
        }
        counter2 = counter2 + 1
      }
      counter2 = 0
    }
    var time2 = System.currentTimeMillis()
    results.foreach(x => println(s"Similarity of ${x._1} is ${x._2}"))
    results.clear
    println(" Execution time jaccard similarity - " + (time2.toDouble - time1) + " ms")

    println("Using Min Hash Signatures")
    val signatureSize = 150
    val random = new Random()
    val hashFunctions = (1 to signatureSize)
      .map(_ => (random.nextInt(), random.nextInt())) //random values
      .map({ case (a, b) => (x: Int) => (a * x + b) % Int.MaxValue }) //generate hash function as ax+b % max_value
      .toList

    time1 = System.currentTimeMillis()
    val signatures = shingles
      .mapValues(generateMinHashSignature(_, hashFunctions)) // get min of each hash function

    counter1 = 0
    counter2 = 0

    val signaturesMap = signatures.collectAsMap()
    for ((k, v) <- signaturesMap) {
      counter1 = counter1 + 1
      for ((k2, v2) <- signaturesMap) {
        if (counter2 >= counter1) {
          results += ((s"$k, $k2", compareSignatures(v, v2)))
        }
        counter2 = counter2 + 1
      }
      counter2 = 0
    }
    time2 = System.currentTimeMillis()
    results.foreach(x => println(s"Similarity of ${x._1} is ${x._2}"))
    results.clear
    println(" Execution time Min Hash Signature - " + (time2.toDouble - time1) + " ms")

    println("Using LSH")
    val t: Double = if (args.length == 0) 0.8 else args(0).toDouble
    time1 = System.currentTimeMillis()
    val signatures1 = shingles
      .mapValues(generateMinHashSignature(_, hashFunctions)) // get min of each hash function
    val signaturesMap1 = signatures.collectAsMap()
    val candidate_pairs = lsh(t, signatureSize, signaturesMap1)
    for ((doc1, doc2) <- candidate_pairs) {
      results += ((s"$doc1, $doc2", compareSignatures(signaturesMap(doc1), signaturesMap(doc2))))
    }

    time2 = System.currentTimeMillis()
    println(s"CANDIDATE PAIRS=$candidate_pairs")
    results.foreach(x => println(s"Similarity of ${x._1} is ${x._2}"))
    results.clear
    println(" Execution time LSH - " + (time2.toDouble - time1) + " ms")
    sc.stop
  }

  //converting documents to shingles of length k
  def createShingles(doc: String, k: Int): Set[String] = {
    (0 until k)
      .flatMap(doc.substring(_).grouped(k))
      .filter(_.length == k)
      .toSet
  }

  //hash shingles
  def hashShingles(shingles: Set[String]): Set[Int] = {
    shingles.map(_.hashCode)
  }

  //Jaccard Similarity
  def compareSets(A: Set[Int], B: Set[Int]): Double = {
    A.intersect(B).size.toDouble / A.union(B).size
  }

  //Applying hash function
  def generateMinHashSignature(doc: Set[Int], functions: List[Int => Int]): List[Int] = {
    functions.map(h => doc.minBy(h(_))) //get minimum value as signature
  }

  //MinHash comparision
  def compareSignatures(A: List[Int], B: List[Int]): Double = {
    A.zip(B).count({ case (a, b) => a == b }).toDouble / A.size.toDouble
  }

  def lsh(t: Double, s: Int, signaturesMap: scala.collection.Map[String, List[Int]]): Set[(String, String)] = {
    // NOTE: rotated matrix, i.e. signatureMatrix(i) is column i
    val bucketMap = MutableMap[Int, ListBuffer[String]]()
    val (b, r) = findParameters(t, s)
    val bucketSize = 10000
    // println(s"t=$t s=$s b=$b r=$r, ${Math.pow(1.0 / b, 1.0 / r)}")
    for ((k, v) <- signaturesMap) {
      for (current_band <- 0 until b) {
        val slice = v.slice(current_band * r, current_band * r + r)
        val bucket = slice.hashCode % bucketSize
        bucketMap(bucket) = bucketMap.getOrElse(bucket, ListBuffer[String]()) += k
      }
    }
    var candidate_pairs = ListBuffer[(String, String)]()
    for (docs: ListBuffer[String] <- bucketMap.values) {
      val pairs: Iterator[ListBuffer[String]] = docs.combinations(2)
      pairs.foreach {
        p: ListBuffer[String] =>
          if (p.head != p(1))
          {
            candidate_pairs += ((p.head, p(1)))
          }
      }
    }
    candidate_pairs.toSet
  }

  //for tuning b and r to reduce false positives using t close to (1/b) ^ (1/r)
  def findParameters(t: Double, s: Int): (Int, Int) = {
    (1 to s)
      .map(x => (x, s / x))
      .filter { case (b, r) => b * r == s }
      .minBy { case (b, r) => Math.abs(t - Math.pow(1.0 / b, 1.0 / r)) }
  }
}
