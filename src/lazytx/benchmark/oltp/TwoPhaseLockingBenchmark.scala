package lazytx.benchmark.oltp

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

class TwoPhaseLockingHotspotBenchmark(normalCount : Int, hotspotCount : Int, hotspotSize : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    val state = Array.ofDim[Int](1000000)
    val locks = Array.ofDim[Lock](1000000)
    
    for(i <- 0 to 1000000 - 1) {
      state(i) = 0
      locks(i) = new ReentrantLock()
    }
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      val writes = Array.ofDim[Int](10)
      
      var i = normalCount + hotspotCount
      while(i > 0) {
        i -= 1
        
        val k = if(i < normalCount)
          rnd.nextInt(dbsize - hotspotSize)
        else
          dbsize - rnd.nextInt(hotspotSize) - 1
        
        /*
        val k = if(i < normalCount)
          hotspotSize + rnd.nextInt(dbsize - hotspotSize)
        else
          rnd.nextInt(hotspotSize)
        */
        
        writes(i) = k
      }
      
      // Sort
      Arrays.sort(writes)
      
      i = 0
      while(i < writes.length) {
        val w = writes(i)
        
        locks(w).lock()
        state(w) += 1
        
        i += 1
      }
      
      i = writes.length
      while(i > 0) {
        i -= 1
        locks(writes(i)).unlock()
      }
      
      //execute(state, locks, writes, 0)
    }
  }
  
  def execute(state : Array[Int], locks : Array[Lock], writes : Array[Int], index : Int) : Unit = {
    if(index < writes.length) {
      val i = writes(index)
      locks(i).lock()
      //locks(i).synchronized {
        state(i) += 1
        execute(state, locks, writes, index + 1)
      //}
      locks(i).unlock()
    }
  }
}

class TwoPhaseLockingRandomAccessBenchmark extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = Array.ofDim[Int](dbsize)
    val locks = Array.ofDim[Lock](dbsize)
    
    for(i <- 0 to dbsize - 1) {
      state(i) = 0
      locks(i) = new ReentrantLock()
    }
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      val writes = Array.ofDim[Int](txsize)
      var i = 0
      while(i < txsize) {
        val k = rnd.nextInt(dbsize)
        writes(i) = k
        i += 1
      }
      
      // Sort
      Arrays.sort(writes)
      
      if(rnd.nextDouble() < writeRatio) {
        i = 0
        while(i < writes.length) {
          val w = writes(i)
          
          locks(w).lock()
          state(w) += 1
          
          i += 1
        }
      } else {
        i = 0
        var sum = 0
        while(i < writes.length) {
          val w = writes(i)
          
          locks(w).lock()
          sum += state(w)
          
          i += 1
        }
      }
      
      i = writes.length
      while(i > 0) {
        i -= 1
        locks(writes(i)).unlock()
      }
      
      //execute(state, locks, writes, 0)
    }
  }
  
  def execute(state : Array[Int], locks : Array[Lock], writes : Array[Int], index : Int) : Unit = {
    if(index < writes.length) {
      val i = writes(index)
      locks(i).lock()
      //locks(i).synchronized {
        state(i) += 1
        execute(state, locks, writes, index + 1)
      //}
      locks(i).unlock()
    }
  }
}

class TwoPhaseLockingGenericBenchmark extends GenericBenchmark {
  def workload(dbsize : Int, updateCount : Int, keyReadCount : Int, valueReadCount : Int, constraintCount : Int) = {
    val state = Array.ofDim[Int](dbsize)      // false sharing
    val locks = Array.ofDim[Lock](dbsize)
    
    for(i <- 0 until dbsize) {
      state(i) = 0
      locks(i) = new ReentrantLock()
    }
    
    () => {
      var success = false
      
      val rnd = ThreadLocalRandom.current()
      val reads = Array.ofDim[Int](keyReadCount).map(_ => rnd.nextInt(dbsize))
      
      while(!success) {
        var abort = false
        
        var locations = Array.ofDim[Int](keyReadCount + updateCount + valueReadCount).map(_ => rnd.nextInt(dbsize))
        var reads = Array.ofDim[Int](keyReadCount)
        
        // Optimistic reads
        var i = 0
        while(i < keyReadCount && !abort) {
          reads(i) = state(locations(i))
          if(!abort) i += 1
        }
        
        if(!abort) {
          val updates = Array.ofDim[Int](updateCount + valueReadCount).map(_ => rnd.nextInt(dbsize))  
          Arrays.sort(updates)
          
          var remainingConstraints = constraintCount
          var remainingUpdates = updates.size
          var constraint = 0
          
          // TODO: check optimistic reads
          
          for(k <- updates) {
            locks(k).lock()
            
            if(rnd.nextDouble() <= (remainingConstraints.toDouble / remainingUpdates)) {
              constraint += state(k)
              remainingConstraints -= 1
            }
            
            remainingUpdates -= 1
          }
          
          if(constraint % 2 == 0) {
            for(k <- updates) {
              state(k) += 1
            }
          }
          
          for(k <- updates) {
            locks(k).unlock()
          }
        } 
        
        var j = 0
        while(j < i) {
          locks(reads(j)).unlock()
          j += 1
        }
        
        success = !abort
      }
    }
  }
}