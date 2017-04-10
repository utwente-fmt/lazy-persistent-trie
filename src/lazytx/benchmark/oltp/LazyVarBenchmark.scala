package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import lazyvar.LazyTransaction
import lazyvar.LazyTransactionThunk
import lazyvar.LazyVar
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger

class LazyVarHotspotBenchmark(normalCount : Int, hotspotCount : Int, hotspotSize : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    var data = Array.ofDim[LazyVar[Int]](1000000)
    for(i <- 0 to data.length - 1)
      data(i) = new LazyVar(0)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      val tx = new LazyTransaction()
      val f = new LazyTransactionThunk(tx, (v : Int) => v + 1)
      
      val writes = Array.ofDim[LazyVar[Int]](normalCount)
      val h1 = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
      val h2 = dbsize - hotspotSize + rnd.nextInt(hotspotSize)
      
      if(h1 == h2) {
        data(h1).queue(_ + 2)(tx)
      } else {
        data(h1).queue(f)
        data(h2).queue(f)
      }
      
      var i = normalCount
      while(i > 0) {
        i -= 1
        
        val k = rnd.nextInt(dbsize - hotspotSize)
        val lvar = data(k)
        lvar.queue(f)
        writes(i) = lvar
      }

      // Commit
      tx.commit()
      
      // Force
      //if(data(h1).index >= 64)
        data(h1).force()(tx)
        
      if(h2 != h1)
        //if(data(h2).index >= 64)
          data(h2).force()(tx)
      
      i = writes.length
      while(i > 0) {
        i -= 1
        writes(i).force()(tx)
      }
    }
  }
}

class LazyVarRandomAccessBenchmark() extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    var data = Array.ofDim[LazyVar[Int]](1000000)
    for(i <- 0 to data.length - 1)
      data(i) = new LazyVar(0)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      val tx = new LazyTransaction()
      val writes = Array.ofDim[LazyVar[Int]](txsize)
      
      if(rnd.nextDouble() < writeRatio) {
        val f = new LazyTransactionThunk(tx, (v : Int) => v + 1)
        
        // Queue thunks
        var i = txsize
        
        while(i > 0) {
          i -= 1
          val k = rnd.nextInt(dbsize)
          val lvar = data(k)
          lvar.queue(f)
          writes(i) = lvar
        }
      } else {
        val sum = new AtomicInteger(0);
        val f = new LazyTransactionThunk(tx, (v : Int) => { sum.addAndGet(v); v })
        
        // Queue thunks
        var i = txsize
        while(i > 0) {
          i -= 1
          val k = rnd.nextInt(dbsize)
          val lvar = data(k)
          lvar.queue(f)
          writes(i) = lvar
        }
      }
      
      // Commit
      tx.commit()
      
      // Force
      var i = txsize
      while(i > 0) {
        i -= 1
        writes(i).force()(tx)
      }
      
    }
  }
}