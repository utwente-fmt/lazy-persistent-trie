package lazytx.benchmark.oltp

import lazytx.State
import lazytrie.test.Util
import java.util.concurrent.ThreadLocalRandom
import laziness.Lazy
import laziness.Value
import lazytrie.Map

class BlockingStateUpdate extends SimpleBenchmark {  
  def workload(dbsize : Int) : () => Unit = {
    val state = new State(Util.createIntMap(dbsize, 5))
    
    () => {
      val k = ThreadLocalRandom.current().nextInt(dbsize)
      val ns = state.update(s => s.replace(k, (v : Option[Int]) => v.map(_ + 1)))
      ns.force(k)
    }
  }
}

class NonBlockingStateUpdate extends SimpleBenchmark {  
  def workload(dbsize : Int) : () => Unit = {
    val state = new State(Util.createIntMap(dbsize, 5))
    
    () => {
      val k = ThreadLocalRandom.current().nextInt(dbsize)
      val ns = state.updateNonBlocking(s => s.replace(k, (v : Option[Int]) => v.map(_ + 1)))
      ns.force(k)
    }
  }
}

class PreparedBlockingStateUpdate extends SimpleBenchmark {  
  def workload(dbsize : Int) : () => Unit = {
    val state = new State(Util.createIntMap(dbsize, 5))
    val context = state.get.context
    
    () => {
      val k = ThreadLocalRandom.current().nextInt(dbsize)
      
      val f = Map.replace(context, k, (v : Option[Int]) => v.map(_ + 1))
      val ns = state.update(f)
      ns.force(k)
    }
  }
}

class ForceRootBlockingStateUpdate extends SimpleBenchmark {  
  def workload(dbsize : Int) : () => Unit = {
    val state = new State(Util.createIntMap(dbsize, 5))
    val context = state.get.context
    
    () => {
      val k = ThreadLocalRandom.current().nextInt(dbsize)
      
      val f = Map.replace(context, k, (v : Option[Int]) => v.map(_ + 1))
      val ns = state.update(s => {
        var r = f(s)
        r.forceRoot
        r
      })
      ns.force(k)
    }
  }
}