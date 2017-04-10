package lazytx.benchmark.batch

import java.util.HashMap
import java.util.concurrent.ThreadLocalRandom
import lazytrie.Map
import lazytrie.test.Util
import lazytx.MapDiff
import lazytx.State
import java.util.HashSet

class RandomAccessTask(dbsize : Int, txsize : Int) extends Task[Map[Int,Int], MapDiff[Int,Int]] {
  def enqueue(diff : MapDiff[Int,Int]) = {
    val rnd = ThreadLocalRandom.current()

    val keys = scala.collection.mutable.Set[Int]()
    val ka = Array.ofDim[Int](txsize)
    var ki = 0
    
    while(ki < txsize) {
      val k = rnd.nextInt(dbsize)
      if(!keys.contains(k)) {
        keys.add(k)
        ka(ki) = k
        ki += 1
      }
    }

    val r0 = diff.read(ka(0))
    for(i <- 0 until ka.length - 1) {
      val r = diff.read(ka(i + 1))
      diff.update(ka(i), _ => r())
    }
    diff.update(ka(ka.length - 1), _ => r0())
  }
    
  def result(state : Map[Int,Int]) = {
  }
}

class RandomAccessWorkload(state : State[Map[Int,Int]], dbsize : Int, txsize : Int, branchWidth : Int = 5) extends Workload[Map[Int,Int], MapDiff[Int,Int]] {
  val diff = state.get.diff
  
  def emptyDiff = diff.empty
  def makeTask = new RandomAccessTask(dbsize, txsize : Int)
}