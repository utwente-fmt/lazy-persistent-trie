package lazytx.benchmark.batch

import lazytx.MapDiff
import java.util.concurrent.ThreadLocalRandom
import lazytx.State
import lazytrie.test.Util
import laziness.Lazy
import lazytrie.Map

class UnconditionalHotspotTask(dbsize : Int, hotspotSize : Int) extends Task[Map[Int,Int], MapDiff[Int,Int]] {
  def enqueue(diff : MapDiff[Int,Int]) = {
    val rnd = ThreadLocalRandom.current()

    val ka = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    var kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    while(ka == kb) {
      kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    }
    
    for(i <- 1 to 8)
      diff.update(rnd.nextInt(dbsize - hotspotSize), _.map(_ + 1))
      
    diff.update(ka, _.map(_ + 1))
    diff.update(kb, _.map(_ + 1))
  }
    
  def result(state : Map[Int,Int]) = {}
}

class BigConditionHotspotTask(dbsize : Int, hotspotSize : Int) extends Task[Map[Int,Int], MapDiff[Int,Int]] {
  def enqueue(diff : MapDiff[Int,Int]) = {
    val rnd = ThreadLocalRandom.current()

    val ka = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    var kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    while(ka == kb) {
      kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    }
    
    // Conditional, all
    val ks = Array.ofDim[Int](8)
    val rd = Array.ofDim[() => Option[Int]](8)
    
    for(i <- 0 to 7) {
      ks(i) = rnd.nextInt(dbsize - hotspotSize)
      rd(i) = diff.read(ks(i))
    }
    
    val fa = diff.read(ka)
    val fb = diff.read(kb)
    
    val check = Lazy.optimistic(() => {
      val pivot = ThreadLocalRandom.current().nextInt(7)
      
      for(i <- pivot to 7)
        rd(i)().get
      
      for(i <- 0 to pivot - 1)
        rd(i)().get
        
      if(ThreadLocalRandom.current().nextBoolean) {
        fa().get > fb().get
      } else {
        fb().get < fa().get
      }
    })
    
    for(i <- 0 to 7)
      diff.update(ks(i), _.map(_ + 1), check)
      
    diff.update(ka, _.map(_ + 1), check)
    diff.update(kb, _.map(_ + 1), check)
  }
    
  def result(state : Map[Int,Int]) = {}
}



class UncontendedConditionHotspotTask(dbsize : Int, hotspotSize : Int) extends Task[Map[Int,Int], MapDiff[Int,Int]] {
  def enqueue(diff : MapDiff[Int,Int]) = {
    val rnd = ThreadLocalRandom.current()

    val ka = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    var kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    while(ka == kb) {
      kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    }
    
    val kx = rnd.nextInt(dbsize - hotspotSize)
    val ky = rnd.nextInt(dbsize - hotspotSize)
    
    val fx = diff.read(kx)
    val fy = diff.read(ky)
    
    val check = Lazy.optimistic(() => fx().get > fy().get)
    
    diff.update(kx, _.map(_ + 1), check)    
    diff.update(ky, _.map(_ + 1), check)  
    
    for(i <- 1 to 6)
      diff.update(rnd.nextInt(dbsize - hotspotSize), _.map(_ + 1), check)    
      
    diff.update(ka, _.map(_ + 1), check)
    diff.update(kb, _.map(_ + 1), check)
  }
    
  def result(state : Map[Int,Int]) = {}
}

class ContendedConditionHotspotTask(dbsize : Int, hotspotSize : Int) extends Task[Map[Int,Int], MapDiff[Int,Int]] {
  def enqueue(diff : MapDiff[Int,Int]) = {
    val rnd = ThreadLocalRandom.current()

    val ka = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    var kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    while(ka == kb) {
      kb = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
    }

    val fa = diff.read(ka)
    val fb = diff.read(kb)
    
    val check = Lazy.optimistic(() => fa().get > fb().get)
    
    for(i <- 1 to 8)
      diff.update(rnd.nextInt(dbsize - hotspotSize), _.map(_ + 1), check)
 
    diff.update(ka, _.map(_ + 1), check)
    diff.update(kb, _.map(_ + 1), check)
  }
    
  def result(state : Map[Int,Int]) = {}
}

abstract class HotspotWorkload(state : State[Map[Int,Int]], dbsize : Int, hotspotSize : Int, branchWidth : Int = 5) extends Workload[Map[Int,Int], MapDiff[Int,Int]] {
  val diff = state.get.diff
  def emptyDiff = diff.empty
}

class UnconditionalHotspotWorkload(state : State[Map[Int,Int]], dbsize : Int, hotspotSize : Int, branchWidth : Int = 5) extends HotspotWorkload(state, dbsize, hotspotSize, branchWidth) {
  def makeTask = new UnconditionalHotspotTask(dbsize, hotspotSize)
}

class BigConditionHotspotWorkload(state : State[Map[Int,Int]], dbsize : Int, hotspotSize : Int, branchWidth : Int = 5) extends HotspotWorkload(state, dbsize, hotspotSize, branchWidth) {
  def makeTask = new BigConditionHotspotTask(dbsize, hotspotSize)
}

class UncontendedConditionHotspotWorkload(state : State[Map[Int,Int]], dbsize : Int, hotspotSize : Int, branchWidth : Int = 5) extends HotspotWorkload(state, dbsize, hotspotSize, branchWidth) {
  def makeTask = new UncontendedConditionHotspotTask(dbsize, hotspotSize)
}

class ContendedConditionHotspotWorkload(state : State[Map[Int,Int]], dbsize : Int, hotspotSize : Int, branchWidth : Int = 5) extends HotspotWorkload(state, dbsize, hotspotSize, branchWidth) {
  def makeTask = new ContendedConditionHotspotTask(dbsize, hotspotSize)
}