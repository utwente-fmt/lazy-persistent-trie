package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom

import laziness.Lazy
import lazytrie.Map
import lazytrie.test.Util
import lazytx.State

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
    
    var diff = state.get.empty
    val keys = Array.ofDim[Int](count)
    
    var i = 0
    while(i < count) {
      val k = rnd.nextInt(dbsize)
      val v = rnd.nextInt(1000000)
      diff.putInPlace(k, v)
      keys(i) = k
      i += 1
    }
    val ns = state.update(m => m.write(diff))
    
    if(eager) {
      var j = 0
      ns.forceRoot
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
      diff.putInPlace(k, v)
      keys(i) = k
      i += 1
    }
    val ns = state.update(m => m.write(diff))
    
    if(eager) {
      var j = 0
      ns.forceRoot
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
    val ns = state.update(m => m.replaceRange(_ => ThreadLocalRandom.current().nextInt(10), start, start + count - 1))
    if(eager) {
      ns.forceRoot
      ns.forceRange(start, start + count - 1)
    }
    ns
  }
}

class LazyRandomAccessBenchmark(leafWidth : Int, branchWidth : Int, eager : Boolean) extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, leafWidth, branchWidth))
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        LazyBenchmarkHelper.randomWrite(state, dbsize, txsize, eager)
      } else {
        LazyBenchmarkHelper.randomRead(state.get, dbsize, txsize)
      }
    }
  }
}

class LazySkewedAccessBenchmark(p : Double, leafWidth : Int, branchWidth : Int, eager : Boolean) extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, leafWidth, branchWidth))
    
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

class LazySequentialAccessBenchmark(leafWidth : Int, branchWidth : Int, eager : Boolean) extends SequentialAccessBenchmark {
  def workload(dbsize : Int, count : Int, writeRatio : Double) = {
    val state = new State(Util.createIntMap(dbsize, leafWidth, branchWidth))

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

class LazyRandomReadBenchmark(leafWidth : Int, branchWidth : Int, eager : Boolean) extends OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) = {
    val state = new State(Util.createIntMap(dbsize, leafWidth, branchWidth))

    () => {
      val rnd = ThreadLocalRandom.current()
      var snapshot : Map[Int,Int] = null
      
      val keys = Array.ofDim[Int](writeSize)
      
      var diff = state.get.emptyDiff   
      var i = 0
      while(i < writeSize) {
        val k = rnd.nextInt(dbsize)
        diff.putInPlace(k, _ => Some({
          var sum = 0
          var j = 0
          while(j < readSize) {
            val r = rnd.nextInt(dbsize)
            sum += snapshot.get(r).get
            j += 1
          }
          sum
        }))
        keys(i) = k
        i += 1
      }
      
      val ns = state.update(m => { snapshot = m; m.update(diff) })
      
      if(eager) {
        var j = 0
        ns.forceRoot
        while(j < writeSize) {
          ns.force(keys(j))
          j += 1
        }
      }
    }
  }
}

class LazyOptimisticBenchmark(maxFails : Int, leafWidth : Int, branchWidth : Int, eager : Boolean) extends OptimisticBenchmark {    
  def workload(dbsize : Int, readSize : Int, writeSize : Int) : () => Unit = {
    val state = new State(Util.createIntMap(dbsize, leafWidth, branchWidth))

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
            diff.putInPlace(k, v)
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
              diff.putInPlace(k, v)
              writeKeys(j) = k
              j += 1
            }
            
            s.write(diff)
          })
          
          if(eager) {
            var k = 0
            ns.forceRoot
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