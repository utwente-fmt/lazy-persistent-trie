package lazyvar2

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicLong

object LazyVar {
  var n = new AtomicLong(0L)
  
  def apply[T](v : T) = new LazyVar[T](v)
}

class LazyVar[T](var value : T) {
  val order = LazyVar.n.getAndIncrement 
  var buffer = ArrayBuffer[T => T]()
  val lock = new ReentrantLock()
  
  def force = {
    if(buffer.size > 0) {
      lock.lock()
      for(f <- buffer)
        value = f(value)
      buffer = ArrayBuffer[T => T]() //clear()
      lock.unlock()
    }
  }
}