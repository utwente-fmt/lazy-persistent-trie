package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable._
import lazytx.State
import scala.collection.concurrent.TrieMap

object CTrieHelper { 
  def create(size : Int) = {
    var map = TrieMap[Int,Int]()
    for(i <- 0 to size - 1) {
      map.put(i, 0)
    }
    map
  }
}

class CTrieRandomAccessBenchmark extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(CTrieHelper.create(dbsize))
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        state.update(m => {
          var i = 0
          while(i < txsize) {
            m.put(rnd.nextInt(dbsize), rnd.nextInt(10))
            i += 1
          }
          m
        })
          
      } else {
        val s = state.get.snapshot()
        var i = 0
        var sum = 0
        while(i < txsize) {
          sum += s.get(rnd.nextInt(dbsize)).get
          i += 1
        }
      }
    }
  }
}

class CTrieSequentialAccessBenchmark extends SequentialAccessBenchmark {  
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(CTrieHelper.create(dbsize))
    
    () => {
      val rnd = ThreadLocalRandom.current()
      val start = rnd.nextInt(dbsize - txsize)
      if(rnd.nextDouble() < writeRatio) {
        state.update(m => {
          var i = 0
          while(i < txsize) {
            m.put(start + i, rnd.nextInt(10))
            i += 1
          }
          m
        })
          
      } else {
        val s = state.get.snapshot()
        var i = 0
        var sum = 0
        while(i < txsize) {
          sum += s.get(start + i).get
          i += 1
        }
      }
    }
  }
}