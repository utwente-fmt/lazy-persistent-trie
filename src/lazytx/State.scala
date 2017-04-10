package lazytx

import scala.concurrent.util.Unsafe
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import scala.reflect.ClassTag
import java.util.concurrent.locks.LockSupport
import lazytrie.Map

object State {
  val offset = Unsafe.instance.objectFieldOffset(classOf[State[_]].getDeclaredField("state"));
}

class State[T](var state : T) { 
  def get = state
  
  def read[U](f : T => U) = f(state)
  
  def update(f : T => T) : T = {
    synchronized {
      state = f(state)
      state
    }
  }
  
  def updateSingleThreaded(f : T => T) : T = {
    state = f(state)
    state
  }
  
  def updateNonBlocking(f : T => T) : T = {
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
