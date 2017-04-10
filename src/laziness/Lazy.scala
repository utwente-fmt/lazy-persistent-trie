package laziness

import scala.concurrent.util.Unsafe

/**
 * Convenience methods
 */
object Lazy {
  def delayed[T](f : () => T) = new Delayed(f)
  def optimistic[T](f : () => T) = new OptimisticThunk(new Delayed(f))
  def nonAtomicOptimistic[T](f : () => T) = new NonAtomicOptimisticThunk(new Delayed(f))
  def locking[T](f : () => T) = LockingThunk(new Delayed(f))
}

/**
 * A value of type Lazy[T] represents a value of type T that may be a suspended computation
 */
abstract class Lazy[T] {
  def get : T
  def evaluate : Lazy[T]
  def isEvaluated : Boolean
}

/**
 * Wrapper for evaluated values
 * Note: value classes can directly implement Lazy[T] to avoid the overhead of this wrapper
 */
case class Value[T](value : T) extends Lazy[T] {
  def get = value
  def evaluate = this
  def isEvaluated = true
}

/**
 * Delayed evaluation without memoization
 */
case class Delayed[T](f : () => T) extends Lazy[T] {  
  def get = evaluate.get
  def evaluate = new Value(f())
  def isEvaluated = false
}

/**
 * Guarantees that concurrent evaluation produces reference-equivalent results
 * The delayed procedure may be evaluated multiple times
 */
case class OptimisticThunk[T](var f : Lazy[T]) extends Lazy[T] {
  def get = evaluate.get
  def evaluate = {
    val n = f
    val r = n.evaluate
    if(n eq r)
      r
    else {
      if(Unsafe.instance.compareAndSwapObject(this, OptimisticThunk.offset, n, r))
        r
      else
        f
    }
  }
  def isEvaluated = f.isEvaluated
}

object OptimisticThunk {
  val offset = Unsafe.instance.objectFieldOffset(classOf[OptimisticThunk[_]].getDeclaredField("f"))
}

/**
 * Does NOT guarantee that concurrent evaluation produces reference-equivalent results
 * The delayed procedure may be evaluated multiple times
 * 
 * Note: the signature should be NonAtomicOptimisticThunk[T](@volatile var x : Lazy[T]), but scalac doesnt allow this
 */
case class NonAtomicOptimisticThunk[T](var x : Lazy[T]) extends Lazy[T] {
  @volatile var f = x
  x = null
  
  def get = evaluate.get
  def evaluate = {
    f = f.evaluate
    f
  }
  def isEvaluated = f.isEvaluated
}

/**
 * Guarantees that the delayed procedure is evaluated only once
 * Guarantees that concurrent evaluation produces reference-equivalent results
 */
case class LockingThunk[T](var f : Lazy[T]) extends Lazy[T] {
  def get = evaluate.get
  def evaluate = {
    if(f.isEvaluated)
      f
    else
      this.synchronized ({
        @volatile var temp = f.evaluate
        f = temp
        f
      })
  }
  def isEvaluated = f.isEvaluated
}
