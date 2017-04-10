package offheap

import laziness.Lazy

abstract class OffHeapLazy[T] extends Lazy[OffHeapLazy[T]] with Node[OffHeapLazy[T]] { }

class LazyHeap[T](source : Heap[OffHeapLazy[T]]) extends Heap[OffHeapLazy[T]] { 
  val LOCK_COUNT = 1024
  val locks = (1 to LOCK_COUNT).toArray.map(_ => new Object())
  
  def append(value : OffHeapLazy[T], reserve : Int) : Long = {
    source.append(value, reserve)
  }
  
  def write(pointer : Long, value : OffHeapLazy[T]) : Unit =
    source.write(pointer, value)
  
  def read(pointer : Long) : OffHeapLazy[T] = {
    val thunk = source.read(pointer)
    if(!thunk.isEvaluated) {
      val evaluated = thunk.get
      source.write(pointer, evaluated)
      evaluated
    } else {
      thunk
    }
  }
  
  def commit() = source.commit()
}