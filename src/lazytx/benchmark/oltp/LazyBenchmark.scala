package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import laziness.Lazy
import lazytrie.Map
import lazytrie.test.Util
import lazytx.State
import lazytrie.ComposableMap.toComposableMap
import lazytrie.ComposableMap
import scala.collection.mutable.ArrayBuffer

case class Reference[T](var ref : T)

object LazyBenchmarkHelper {
  def randomRead(state : Map[Int,Int], dbsize : Int, count : Int) = {
    val rnd = ThreadLocalRandom.current()
    var sum = 0
    var i = 0
    while(i < count) {
      sum += state.get(rnd.nextInt(dbsize)).get
      i += 1
    }
  }
  
  def skewedRead(state : Map[Int,Int], dbsize : Int, count : Int, p : Double) = {
    val rnd = ThreadLocalRandom.current()
    var sum = 0
    var i = 0
    while(i < count) {
      sum += state.get(Util.skewedRandom(dbsize, p)).get
      i += 1
    }
  }
  
  def randomWrite(state : State[Map[Int,Int]], dbsize : Int, count : Int, eager : Boolean) = {
    val rnd = ThreadLocalRandom.current()
    
    var diff = state.get.emptyMap[Option[Int] => Option[Int]]
    val cdiff = new ComposableMap(diff)
    val keys = Array.ofDim[Int](count)
    val f = (v : Option[Int]) => Some(v.get + 1)
    
    var i = 0
    while(i < count) {
      val k = rnd.nextInt(dbsize)
      cdiff.composeInPlace(k, f)
      keys(i) = k
      i += 1
    }
    val ns = state.update(m => m.update(diff))
    
    if(eager) {
      var j = 0
      while(j < count) {
        ns.force(keys(j))
        j += 1
      }
    }
    ns
  }
  
  def skewedWrite(state : State[Map[Int,Int]], dbsize : Int, count : Int, p : Double, eager : Boolean) = {
    val rnd = ThreadLocalRandom.current()
    
    var diff = state.get.empty
    val keys = Array.ofDim[Int](count)
    
    var i = 0
    while(i < count) {
      val k = Util.skewedRandom(dbsize, p)
      val v = rnd.nextInt(1000000)
      diff(k) = v
      keys(i) = k
      i += 1
    }
    val ns = state.update(m => m.write(diff))
    
    if(eager) {
      var j = 0
      while(j < count) {
        ns.force(keys(j))
        j += 1
      }
    }
    ns
  }
  
  def seqRead(state : Map[Int,Int], dbsize : Int, count : Int) = {
    val rnd = ThreadLocalRandom.current()
    val start = rnd.nextInt(dbsize - count)
    state.reduceRange(0, (a : Int, b : Int) => a + b, start, start + count - 1)
  }
  
  def seqWrite(state : State[Map[Int,Int]], dbsize : Int, count : Int, eager : Boolean) = {
    val rnd = ThreadLocalRandom.current()
    val start = rnd.nextInt(dbsize - count)
    val ns = state.update(m => m.updateRange(v => Some(v + 1), start, start + count - 1))
    if(eager) {
      ns.forceRange(start, start + count - 1)
    }
    ns
  }
}

class LazyHotspotBenchmark(branchWidth : Int, normalCount : Int, hotspotCount : Int, hotspotSize : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    val emptyDiff = state.get.emptyMap[Option[Int] => Option[Int]]
    val f = (v : Option[Int]) => Some(v.get + 1)
    
    () => {
      val rnd = ThreadLocalRandom.current()
     
      val diff = emptyDiff.empty
      //val keys = Array.ofDim[Int](normalCount + hotspotCount)
      
      var i = normalCount + hotspotCount
      while(i > 0) {
        i -= 1
        val k = if(i < normalCount)
          rnd.nextInt(dbsize - hotspotSize)
        else
          dbsize - hotspotSize + rnd.nextInt(hotspotSize)
          
        diff.composeInPlace(k, f)
        //keys(i) = k
      }

      val ns = state.update(_.update(diff))
      
      ns.forceRoot
      ns.force(diff)
    }
  }
}

class LazyUpdateBenchmark(branchWidth : Int, count : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    val emptyDiff = state.get.emptyMap[Option[Int] => Option[Int]]
    val f = (v : Option[Int]) => Some(v.get + 1)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      val diff = emptyDiff.empty
      
      var i = count
      while(i > 0) {
        i -= 1
        val k = rnd.nextInt(dbsize)
        diff.composeInPlace(k, f)
      }

      val ns = state.update(_.update(diff))
      ns.force(diff)
    }
  }
}

class LazyReadUpdateBenchmark(branchWidth : Int, count : Int) extends SimpleBenchmark {
  def workload(dbsize : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    val gtx = state.get.diff
    val f = (v : Option[Int]) => v
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      val tx = gtx.empty
      
      var i = count
      while(i > 0) {
        i -= 1
        
        val r = tx.read(rnd.nextInt(dbsize))
        tx.update(rnd.nextInt(dbsize), (v : Option[Int]) => r())
      }

      val ns = state.update(tx.apply(_))
      tx.force(ns)
    }
  }
}

class LazyReadUpdateSimplifiedBenchmark(branchWidth : Int, count : Int) extends SimpleBenchmark {
  val queue = new ThreadLocal[ArrayBuffer[Map[Int, Option[Int] => Option[Int]]]]
  
  def workload(dbsize : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    val emptyDiff = state.get.emptyMap[Option[Int] => Option[Int]]
    val f = (v : Option[Int]) => Some(v.get + 1)
    
    val initialState = state.get
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      val diff = emptyDiff.empty
      
      var snapshot : Map[Int,Int] = null
      
      var i = count
      while(i > 0) {
        i -= 1
        val k = rnd.nextInt(dbsize)
        val r = rnd.nextInt(dbsize)
        diff.composeInPlace(k, (v : Option[Int]) => snapshot.get(r)) 
      }

      val ns = state.update(s => {
        snapshot = s
        s.update(diff)
      })
      
      snapshot.forceRoot
      
      ns.forceRoot
      ns.force(diff)
      
      /*
      snapshot.forceRoot
      
      var myQueue = queue.get
      if(myQueue == null) {
        myQueue = new ArrayBuffer[Map[Int, Option[Int] => Option[Int]]]()
        queue.set(myQueue)
      }
      
      myQueue += diff
      
      if(myQueue.length == 8) {
        ns.force(myQueue.remove(0))
      }
      */
      
      /*
      ns.forceRoot
      ns.force(diff)*/
      
      /*
      if(rnd.nextInt(8) == 0) {
        val b = rnd.nextInt(dbsize / count / 8) * count * 8
        ns.forceRoot
        ns.forceRange(b, b + count * 8)
      }
      */
    }
  }
}

class LazyRandomAccessBenchmark(branchWidth : Int, eager : Boolean) extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    val emptyDiff = state.get.emptyMap[Option[Int] => Option[Int]]
    val f = (v : Option[Int]) => Some(v.get + 1)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        var diff = emptyDiff.empty
        val cdiff = new ComposableMap(diff)
        val keys = Array.ofDim[Int](txsize)
        
        var i = 0
        while(i < txsize) {
          val k = rnd.nextInt(dbsize)
          cdiff.composeInPlace(k, f)
          keys(i) = k
          i += 1
        }
        val ns = state.update(m => m.update(diff))
        
        if(eager) {
          var j = 0
          while(j < keys.length) {
            ns.force(keys(j))
            j += 1
          }
        }
      } else {
        val snapshot = state.get
        var sum = 0
        var i = 0
        while(i < txsize) {
          sum += snapshot(rnd.nextInt(dbsize))
          i += 1
        }
      }
    }
  }
}

class LazyUpdateAllBenchmark(branchWidth : Int, eager : Boolean) extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    
    () => {
      val ns = state.update(_.vmap(v => Some(v + 1)))
      ns.forceAll()
    }
  }
}

class LazySkewedAccessBenchmark(p : Double, branchWidth : Int, eager : Boolean) extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        LazyBenchmarkHelper.skewedWrite(state, dbsize, txsize, p, eager)
      } else {
        LazyBenchmarkHelper.skewedRead(state.get, dbsize, txsize, p)
      }
    }
  }
}

class LazySequentialAccessBenchmark(branchWidth : Int, eager : Boolean) extends SequentialAccessBenchmark {
  def workload(dbsize : Int, count : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))

    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        LazyBenchmarkHelper.seqWrite(state, dbsize, count, eager)
      } else {
        LazyBenchmarkHelper.seqRead(state.get, dbsize, count)
      }
    }
  }
}

class LazyRandomReadBenchmark(branchWidth : Int, eager : Boolean) extends OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))

    () => {
      val rnd = ThreadLocalRandom.current()
      var snapshot : Map[Int,Int] = null
      
      val keys = Array.ofDim[Int](writeSize)
      
      var diff = state.get.emptyMap[Option[Int] => Option[Int]] 
      var i = 0
      while(i < writeSize) {
        val k = rnd.nextInt(dbsize)
        diff(k) = (_ : Option[Int]) => Some({
          var sum = 0
          var j = 0
          while(j < readSize) {
            val r = rnd.nextInt(dbsize)
            sum += snapshot.get(r).get
            j += 1
          }
          sum
        })
        keys(i) = k
        i += 1
      }
      
      val ns = state.update(m => { snapshot = m; m.update(diff) })
      
      if(eager) {
        var j = 0
        while(j < writeSize) {
          ns.force(keys(j))
          j += 1
        }
      }
    }
  }
}

class LazyOptimisticBenchmark(maxFails : Int, branchWidth : Int, eager : Boolean) extends OptimisticBenchmark {    
  def workload(dbsize : Int, readSize : Int, writeSize : Int) : () => Unit = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))

    () => {
      val rnd = ThreadLocalRandom.current()
      
      // Allocate buffers
      val readKeys = Array.ofDim[Int](readSize)
      val readVals = Array.ofDim[Option[Int]](readSize)
      val writeKeys = Array.ofDim[Int](writeSize)
      
      var fails = 0
      
      var success = false
      while(!success) {
        if(fails < maxFails) {
          // Obtain a snapshot
          val s = state.get
          
          // Read optimistically
          var i = 0
          while(i < readSize) {
            val k = rnd.nextInt(dbsize)
            readKeys(i) = k
            readVals(i) = s.get(k)
            i += 1
          }
          
          // Construct check
          var commitState : Map[Int,Int] = null

          var low = 0
          var high = writeSize
          var result = true
          
          val check = Lazy.optimistic(() => {
            if(commitState eq s) {
              true 
            } else {
              if(low == 0) {
                var i = low
                while(i < high && result) {
                  if(commitState.get(readKeys(i)) != readVals(i))
                    result = false
                  i += 1
                  low = i
                }
              } else {
                var i = high
                while(i > low && result) {
                  i -= 1
                  if(commitState.get(readKeys(i)) != readVals(i))
                    result = false
                  high = i
                }
              }
              result
            }
          })
          
          // Construct write diff
          var diff = state.get.empty
          //var writes = Map[Int,Int]()
          var j = 0
          while(j < writeSize) {
            val k = rnd.nextInt(dbsize)
            val v = rnd.nextInt(1000000)
            diff(k) = v
            writeKeys(j) = k
            //writes += k -> v
            j += 1
          }
          
          // Commit
          val ns = state.update(m => { commitState = m; m.writeConditional(diff, check) })
          //val ns = state.update(m => { commitState = m; if(check.get) m.write(diff) else m })
          
          // Force evaluation of the check
          success = check.get
          
          // Force evaluation of all written keys (note that we have to do this regardless of whether we succeeded: we need to get rid of thunks in the state)
          if(eager) {
            var k = 0
            while(k < writeSize) {
              ns.force(writeKeys(k))
              k += 1
            }
          }
          
          if(!success) {
            fails += 1
          }
          
          // Correctness check
          /*
          if(success) {
            var ra = true
            var i = 0
            while(i < readSize && ra) {
              ra = (commitState.get(readKeys(i)) == readVals(i))
              i += 1
            }
  
            var rb = true
            i = 0
            while(i < readSize && rb) {
              rb = (ns.get(writeKeys(i)).get == writes(writeKeys(i)))
              i += 1
            }
            
            if(!ra || !rb)
              println("ERROR: Invalid update")
          } else {
            if(ns.toMap != commitState.toMap)
              println("ERROR: Invalid abort")
          }
          */
        } else {
          // Execute in commit phase
          val ns = state.update(s => {
            var i = 0
            while(i < readSize) {
              val k = rnd.nextInt(dbsize)
              s.get(k)
              i += 1
            }
            
            // Construct write diff
            var diff = state.get.empty
            var j = 0
            while(j < writeSize) {
              val k = rnd.nextInt(dbsize)
              val v = rnd.nextInt(1000000)
              diff(k) = v
              writeKeys(j) = k
              j += 1
            }
            
            s.write(diff)
          })
          
          if(eager) {
            var k = 0
            while(k < writeSize) {
              ns.force(writeKeys(k))
              k += 1
            }
          }
          
          success = true
        }
      }
    }
  }
}

// TODO: assign constraint keys, do lazy reads
class LazyGenericBenchmark(branchWidth : Int = 5) extends GenericBenchmark {
  def workload(dbsize : Int, updateCount : Int, keyReadCount : Int, valueReadCount : Int, constraintCount : Int) = {
    val state = new State(Util.createIntMap(dbsize, branchWidth))
    
    () => {
      var success = false
      val readKeys = Array.ofDim[Int](keyReadCount)
      
      while(!success) {
        val readVals = Array.ofDim[Option[Int]](keyReadCount)
        
        val snapshot = state.get
        for(i <- 0 until keyReadCount) {
          readVals(i) = snapshot.get(readKeys(i))
        }
        
        val constraintKeys = Array.ofDim[Int](constraintCount)
        
        var commitState : Map[Int,Int] = null
        val serializable = Lazy.optimistic(() => (0 until keyReadCount).forall(i => {
          commitState.get(readKeys(i)) == readVals(i)
        }))
        val consistent = Lazy.optimistic(() => constraintKeys.map(k => commitState(k)).sum % 2 == 0)
        
        val diff = snapshot.emptyMap[Option[Int] => Option[Int]]
        for(i <- 0 until updateCount) {
          diff(i) = (x : Option[Int]) => {
            if(serializable.get && consistent.get)
              x.map(_ + 1)
            else
              x
          }
        }
        
        val ns = state.update(s => {
          commitState = s
          s.update(diff)
        })
        
        ns.force(diff)
        
        success = serializable.get
      }
    }
  }
}