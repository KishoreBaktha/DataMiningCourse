import java.util.Random

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

object Triest {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging // to disable unnecessary logging and improve execution time

    val input = env.readTextFile("data/arenas-pgp/out.arenas-pgp")

    val s = if (args.size > 1) args(1).toInt else 5000 // size of sample
    val w = if (args.size > 2) args(2).toInt else s // size of window for printing results
    val impl =
      if (args.size == 0) {
        println("Using TRIEST-IMPR")
        new TriestImpr(s)
      }
      else {
        val str = args(0).toUpperCase
        if (str equals "BASE") {
          println("Using TRIEST-BASE")
          new TriestBase(s)
        }
        else {
          println("Using TRIEST-IMPR")
          new TriestImpr(s)
        }
      }
    println(s"M: $s, w: $w")
    val countGlobalTriangles = input
      .filter(line => line.nonEmpty && !line.startsWith("%")) // remove irrelevant lines from input file chosen
      .map(_.split("\\s+") match { case Array(u, v) => (u.toInt, v.toInt) }) // convert lines to edge tuples
      .map(impl).setParallelism(1) // to run in sequence

    // Print number of triangles every w edges
    countGlobalTriangles
      .countWindowAll(w)
      .max(0)
      .print().setParallelism(1) //to print sequentially

    // Run pipeline
    env.execute("Approximation of global triangles from stream")

  }


  class TriestImpr(M: Int) extends RichMapFunction[(Int, Int), Double] {
    private val reservoirSample = ArrayBuffer[(Int, Int)]() // reservoir of size M for reservoir sampling
    private val random = new Random()
    private var t = 0
    private var tau: Double = 0

    override def map(newEdge: (Int, Int)): Double = {
      t += 1
      val incrementVal = Math.max(1, ((t-1)*(t-2)).toDouble/(M*(M-1)))
      updateCounter(incrementVal, newEdge)
      if (t <= M) {
        // Append new edge and update counter
        reservoirSample.append(newEdge)
      }
      else if (flipBiasedCoin(M.toFloat / t)) {
        val index = random.nextInt(M)
        val replaceEdge = reservoirSample(index)
        reservoirSample.update(index, newEdge)
      }

      tau
    }

    private def updateCounter(difference: Double, edge: (Int, Int)): Unit = {
      //find common neighbours
      val neighbours = (u: Int) => reservoirSample.collect({ case (v1, v2) if v1 == u => v2; case (v1, v2) if v2 == u => v1 }).toSet
      val commonNeighbours = neighbours(edge._1) intersect neighbours(edge._2)
      tau += commonNeighbours.size * difference
      //println(" For edge " + edge._1 + "and " + edge._2  + " cng " + commonNeighbours + " tau " + tau )
    }

    private def flipBiasedCoin(prob: Float) = random.nextFloat() < prob
  }

  class TriestBase(M: Int) extends RichMapFunction[(Int, Int), Double] {
    private val reservoirSample = ArrayBuffer[(Int, Int)]() // reservoir of size M for reservoir sampling
    private val random = new Random()
    private var t = 0
    private var tau = 0

    override def map(newEdge: (Int, Int)): Double = {
      t += 1
      if (t <= M) {
        // Append new edge and update counter
        reservoirSample.append(newEdge)
        updateCounter(+1, newEdge)
      }
      else if (flipBiasedCoin(M.toFloat / t)) {

        val index = random.nextInt(M)
        val replaceEdge = reservoirSample(index)

        reservoirSample.update(index, (0, 0))
        updateCounter(-1, replaceEdge) //remove edge

        reservoirSample.update(index, newEdge)
        updateCounter(+1, newEdge)  // add new edge
      }

      // calculating et as given in paper
      val num = t.toLong * (t.toLong - 1) * (t.toLong - 2) //numerator
      val den = M.toLong * (M.toLong - 1) * (M.toLong - 2)  //denominator
      val et = Math.max(1, num.toDouble / den)

      et * tau //final approximation
    }

    private def updateCounter(difference: Int, edge: (Int, Int)): Unit = {
      //find common neighbours
      val neighbours = (u: Int) => reservoirSample.collect({ case (v1, v2) if v1 == u => v2; case (v1, v2) if v2 == u => v1 }).toSet
      val commonNeighbours = neighbours(edge._1) intersect neighbours(edge._2)
      tau += commonNeighbours.size * difference
      //println(" For edge " + edge._1 + "and " + edge._2  + " cng " + commonNeighbours + " tau " + tau )
    }

    private def flipBiasedCoin(prob: Float) = random.nextFloat() < prob
  }



}
