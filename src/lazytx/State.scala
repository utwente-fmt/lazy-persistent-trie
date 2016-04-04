package lazytx

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.util.Unsafe

object State {
  val offset = Unsafe.instance.objectFieldOffset(classOf[State[_]].getDeclaredField("state"));
}

class State[T](var state : T) {
  def get = state
  
  def read[U](f : T => U) = f(state)
  
  def update(f : T => T) : T = {
    this.synchronized {
      state = f(state)
      state
    }
  }
  
  def updateSingleThreaded(f : T => T) : T = {
    state = f(state)
    state
  }
  
  def updateNonblocking(f : T => T) : T = {
    while(true) {
      val s = state
      val result = f(s)
      if(Unsafe.instance.compareAndSwapObject(this, State.offset, s, result))
        return result
    }
    throw new Exception
  }
  
  def run[U](f : T => (T, U)) : (T, U) = {
    this.synchronized {
      var r = f(state)
      state = r._1
      (r._1, r._2)
    }
  }
}