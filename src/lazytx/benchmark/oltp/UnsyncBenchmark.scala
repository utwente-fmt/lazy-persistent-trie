package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger

class UnsyncHotspotBenchmark(normalCount : Int, hotspotCount : Int, hotspotSize : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    val state = Array.ofDim[AtomicInteger](1000000)
    for(i <- 0 to 1000000 - 1) {
      state(i) = new AtomicInteger(0)
    }
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      var i = normalCount + hotspotCount
      while(i > 0) {
        i -= 1
        val k = if(i < normalCount)
          rnd.nextInt(dbsize - hotspotSize)
        else
          dbsize - hotspotSize + rnd.nextInt(hotspotSize)
        
        state(k).incrementAndGet()
      }
    }
  }
}