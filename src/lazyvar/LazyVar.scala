package lazyvar

import java.util.Arrays
import java.util.Comparator
import lazytx.benchmark.oltp.Benchmark
import scala.concurrent.util.Unsafe

object Counter {
  var count = 0L
  
  def commit(tx : LazyTransaction) {
    this.synchronized {
      count += 1L
      tx.commitId = count
    }
  }
}

object LazyTransaction {
  val offset = Unsafe.instance.objectFieldOffset(classOf[LazyTransaction].getDeclaredField("commitId"))
}

class LazyTransaction(var commitId : Long = Long.MaxValue) {
  def commit() = {
    /*
    if(commitId == Long.MaxValue - 1)
      Counter.commit(this)
    else if(!Unsafe.instance.compareAndSwapObject(this, LazyTransaction.offset, Long.MaxValue, 0))
      Counter.commit(this)
    */
    
    
    synchronized {
      if(commitId == Long.MaxValue - 1)
        Counter.commit(this)
      else
        commitId = 0
    }
    
  }
  
  def setGlobalCounter() = {
    /*
    if(commitId == Long.MaxValue)
      Unsafe.instance.compareAndSwapObject(this, LazyTransaction.offset, Long.MaxValue, Long.MaxValue - 1)
    */
    
    if(commitId == Long.MaxValue) {
      synchronized {
        if(commitId == Long.MaxValue) {
          commitId = Long.MaxValue - 1
        }
      }
    }
    
  }
}

object LazyTransactionThunk {
  val comparator = new Comparator[LazyTransactionThunk[Nothing]] {
    override def compare(a : LazyTransactionThunk[Nothing], b : LazyTransactionThunk[Nothing]) = {
      val x = a.commitId
      val y = b.commitId
      
      if(x < y)
        -1
      else if(x > y)
        1
      else 
        0
    }
  }
}

class LazyTransactionThunk[T](val transaction : LazyTransaction, val f : T => T) {
  @inline def commitId = transaction.commitId
}

class LazyVar[T](init : T) {
  var value = init
  var thunks = Array.ofDim[LazyTransactionThunk[T]](1)
  var index = 0
  var version = 0L
  var nextVersion = version
  
  def queue(f : T => T)(implicit tx : LazyTransaction) : Unit = {
    queue(new LazyTransactionThunk(tx, f))
  }
  
  def queue(thunk : LazyTransactionThunk[T]) : Unit = {
    var ind = 0
    
    synchronized {      
      ind = index
      if(index == 1)
        thunks(0).transaction.setGlobalCounter()
          
      if(index == thunks.length) {
        var nthunks = Array.ofDim[LazyTransactionThunk[T]](thunks.length * 2)
        System.arraycopy(thunks, 0, nthunks, 0, thunks.length)
        thunks = nthunks
      }
      
      thunks(index) = thunk
      index += 1      
    }
      
    if(ind > 0) {
      thunk.transaction.setGlobalCounter()
    }
  }
  
  def force()(implicit tx : LazyTransaction) = {
    if(index > 0 && (tx.commitId == 0 || version < tx.commitId)) {
      synchronized {        
        if(index == 1 && (thunks(0).transaction eq tx)) {
          value = thunks(0).f(value)
          version = Math.max(version, thunks(0).commitId)
          thunks(0) = null
          
          index = 0
        } else if(index > 0) {
          var maxCommitId = tx.commitId
          
          // Move all committed thunks to the back
          var low = 0
          var high = index - 1
          while(low < high) {
            while(low < high && thunks(low).commitId > maxCommitId)
              low += 1
            while(low < high && thunks(high).commitId <= maxCommitId)
              high -= 1
            if(low < high) {
              val t = thunks(low)
              thunks(low) = thunks(high)
              thunks(high) = t
            }
          }
          
          // Find index of first committed thunk
          var begin = if(thunks(low).commitId > maxCommitId) {
            low + 1
          } else {
            low
          }  
          
          // Sort committed thunks
          if(index - begin > 1)
            Arrays.sort(thunks.asInstanceOf[Array[LazyTransactionThunk[Nothing]]], begin, index, LazyTransactionThunk.comparator)

          // Execute committed thunks
          var v = value
          var i = begin
          while(i < index) {
            v = thunks(i).f(v)
            thunks(i) = null
            i += 1
          }
          
          version = Math.max(version, maxCommitId)
          
          // Remove committed thunks
          index = begin
            
          value = v
        }
      }
    }
  }
}

object LazyVarTest {  
  def main(args : Array[String]) : Unit = {
    val a = new LazyVar(0)
    val b = new LazyVar(0)
    
    while(true) {
      Benchmark.benchmark(64, 10000, () => {
        var ra = 0
        var rb = 0
        
        val tx = new LazyTransaction()
        
        a.queue((v : Int) => {
          ra = v
          v + 1
        })(tx)
        
        b.queue((v : Int) => {
          rb = v
          v + 1
        })(tx)
        
        tx.commit()
        
        a.force()(tx)
        b.force()(tx)
        
        if(ra != rb)
          println("INCORRECT: " + ra + " " + rb)
      })
      print(".")
    }
  }
}