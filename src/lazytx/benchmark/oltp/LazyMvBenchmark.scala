package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import laziness.Lazy
import lazytrie.Map
import lazytrie.test.Util
import lazytx.State
import lazytrie.ComposableMap.toComposableMap
import lazytrie.ComposableMap

class LazyMvHotspotBenchmark(branchWidth : Int, eager : Boolean, normalCount : Int, hotspotCount : Int, hotspotSize : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    val key = state.get.context.key
    val f = (v : Option[Int]) => Some(v.get + 1)
    
    () => {
      /*
      val tx = new Diff[Int,Int](key)
      
      val rnd = ThreadLocalRandom.current()
      
      var i = normalCount + hotspotCount
      while(i > 0) {
        i -= 1
        val k = if(i < normalCount)
          dbsize - hotspotSize + rnd.nextInt(hotspotSize)
        else
          rnd.nextInt(dbsize - hotspotSize)
          
        tx.update(k, f)
      }
      * 
      */

      //tx.commit(state)
    }
  }
}