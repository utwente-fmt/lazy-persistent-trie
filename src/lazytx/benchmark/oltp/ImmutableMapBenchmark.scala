package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import lazytx.State

object ImmutableMapHelper {
  def create(size : Int) = {
    var map = Map[Int,Int]()
    for(i <- 0 to size - 1) {
      map += i -> 0
    }
    map
  }
}

class ImmutableMapRandomAccessBenchmark extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(ImmutableMapHelper.create(dbsize))
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        state.update(m => {
          var current = m
          var i = 0
          while(i < txsize) {
            current += (rnd.nextInt(dbsize) -> rnd.nextInt(10))
            i += 1
          }
          current
        })
          
      } else {
        val s = state.get
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

class ImmutableMapSequentialAccessBenchmark extends SequentialAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(ImmutableMapHelper.create(dbsize))
    
    () => {
      val rnd = ThreadLocalRandom.current()
      val start = rnd.nextInt(dbsize - txsize)
      if(rnd.nextDouble() < writeRatio) {
        state.update(m => {
          var current = m
          var i = 0
          while(i < txsize) {
            current += ((start + i) -> rnd.nextInt(10))
            i += 1
          }
          current
        })
          
      } else {
        val s = state.get
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