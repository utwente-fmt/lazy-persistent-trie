package lazyvar2

import scala.collection.mutable.ArrayBuffer
import laziness.Lazy

class Transaction {
  val updates = ArrayBuffer[(LazyVar[Nothing], Nothing => Nothing)]()
  val optimistic = ArrayBuffer[(Nothing, () => Nothing)]()
  var conditions : Lazy[Boolean] = null
  
  private def conditional[T](f : T => T) : T => T = {
    if(conditions != null) {
      val c = conditions
      x => if(c.get) f(x) else x
    } else {
      f
    }
  }
  
  def update[T](v : LazyVar[T], f : T => T) : Unit = {
    updates += ((v.asInstanceOf[LazyVar[Nothing]], conditional(f).asInstanceOf[Nothing => Nothing]))
  }
  
  def lazyRead[T](v : LazyVar[T]) : () => T = {
    var r : T = null.asInstanceOf[T]
    updates += ((v.asInstanceOf[LazyVar[Nothing]], ((x : T) => { r = x; x }).asInstanceOf[Nothing => Nothing]))
    () => r
  }
  
  def optimisticRead[T](v : LazyVar[T]) : T = {
    val x = v.value
    val lx = lazyRead(v)
    optimistic += ((x.asInstanceOf[Nothing], lx.asInstanceOf[() => Nothing]))
    x
  }
  
  def iff(condition : () => Boolean)(body : => Unit) = {
    if(conditions == null) {
      conditions = Lazy.optimistic(condition)
      body
      conditions = null
    } else {
      val current = conditions
      conditions = Lazy.optimistic(() => current.get && condition())
      body
      conditions = current
    }
  }
  
  def commit = {
    val ou = updates.sortBy(_._1.order)
    
    // lock
    for((v,_) <- ou) {
      v.lock.lock()
    }
    
    // queue
    if(optimistic.length > 0) {
      val check = Lazy.optimistic(() => 
        optimistic.forall({ case (x,lx) => x == lx() })
      )
      
      for((v,f) <- ou) {
        v.buffer += makeChecked[Nothing](f, check)
      }
    } else {
      for((v,f) <- ou) {
        v.buffer += f
      }
    }
    
    // unlock
    for((v,_) <- ou) {
      v.lock.unlock()
    }
    
    // force
    for((v,_) <- ou) {
      v.force
    }
  }
  
  def commitEager = {
    val ou = updates.sortBy(_._1.order)
    
    // lock
    for((v,_) <- ou) {
      v.lock.lock()
    }
    
    // queue
    if(optimistic.length > 0) {
      val check = Lazy.optimistic(() => 
        optimistic.forall({ case (x,lx) => x == lx() })
      )
      
      for((v,f) <- ou) {
        v.force
        v.value = makeChecked[Nothing](f, check)(v.value)
      }
    } else {
      for((v,f) <- ou) {
        v.force
        v.value = f(v.value)
      }
    }
    
    // unlock
    for((v,_) <- ou) {
      v.lock.unlock()
    }
  }
  
  def makeChecked[T](f : T => T, check : Lazy[Boolean]) =
    (x : T) => if(check.get) f(x) else x
}