package offheap

class SynchronizedHeap[T <: Node[T]](source : Heap[T]) extends Heap[T] {
  val LOCK_COUNT = 1024
  val locks = (1 to LOCK_COUNT).toArray.map(_ => new Object())
  
  def append(value : T, reserve : Int) : Long =
    source.append(value, reserve)
  
  def write(pointer : Long, value : T) : Unit =
    source.write(pointer, value)
  
  def read(pointer : Long) : T = {
    locks(pointer.toInt % LOCK_COUNT).synchronized {
      source.read(pointer)
    }
  }
  
  def commit() = source.commit()
}
