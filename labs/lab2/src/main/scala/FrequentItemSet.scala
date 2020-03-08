import scala.collection.mutable.{Map, ListBuffer, HashSet}
import scala.io.Source
import util.control.Breaks._

object FrequentItemSet {

  def main(args: Array[String]): Unit = {

    val baskets = Source.fromFile("data/T10I4D100K.dat").getLines
      .map(line => line.split(" ").map(item => item.toInt).toSet)
      .toList

    val sup: Double = if (args.length == 0) 0.5 else args(0).toDouble //support threshold
    val conf: Double = if (args.length <= 1) 0.5 else args(1).toDouble //confidence

    val frequentItemsets = findFrequentItems(baskets, sup)
    println(s"Frequent Itemsets with support=$sup:")
    frequentItemsets.foreach(itemset => println("{" + itemset._1.mkString(", ") + "}"))
    println(s"Rules with confidence=$conf:")
    getRules(conf, frequentItemsets).foreach(x => println(s"{${x._1.mkString(", ")}} -> {${x._2.mkString(", ")}}"))
  }

  def findFrequentItems(baskets: List[Set[Int]], sup: Double): Map[Set[Int], Int] = {
    val items = baskets.reduce(_ ++ _) // get all items
    val s = sup * baskets.size
    var totalItemSets = Map[Set[Int], Int]()

    //recursive function to compute frequent itemsets
    def apriorialg(currentItemSets: List[Set[Int]], k: Int): Unit = {
      val counts = Map[Set[Int], Int]()
      currentItemSets.foreach(x => counts(x) = 0)
      baskets.foreach(basket => currentItemSets.foreach(currentItemSet => if (currentItemSet.subsetOf(basket)) counts(currentItemSet) += 1))
      val keep = counts.filter { case (k, v) => v >= s }
      if (keep.nonEmpty) {
        keep.foreach { case (k, v) => totalItemSets(k) = v }
        var newItems = ListBuffer[Set[Int]]()
        keep.keys.flatten.toSet.subsets(k + 1).foreach { //filter out the items not having all subsets in seen frequent itemsets
          case candidate => {
            breakable {
              for (old <- currentItemSets) {
                if (old.subsetOf(candidate)) {
                  newItems += candidate
                  break
                }
              }
            }
          }
        }
        apriorialg(newItems.toList, k + 1)
      }
    }

    apriorialg(items.map(x => Set(x)).toList, 1)
    totalItemSets
  }

  //function to get association rules
  def getRules(conf: Double, frequentItemsets: Map[Set[Int], Int]): List[(Set[Int], Set[Int])] = {
    var results = ListBuffer[(Set[Int], Set[Int])]()
    var skipItems = Set[Int]()
    for ((itemSet, support) <- frequentItemsets if itemSet.size > 1) { //ignore 1-itemset
      var maxsize_left = 0
      val item_size = itemSet.size
      for (k <- item_size to 1 by -1) {
        itemSet.subsets(k).foreach {
          case left => {
            val left_size = left.size
            if (left_size != 0 && left_size < item_size){
              if (!left.subsetOf(skipItems)){
                val c = support.toDouble/frequentItemsets(left)
                if (c >= conf) {
                  val right = itemSet -- left
                  results += ((left, right))
                }
                else skipItems ++= left
              }
            }
          }
        }
      }
    }
    results.toList
  }
}
