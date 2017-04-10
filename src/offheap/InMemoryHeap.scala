package offheap

import java.util.HashMap

class InMemoryHeap[T <: Node[T]] extends Heap[T] {
  val data = new HashMap[Long, T]
  var next_id = 0
  
  def append(node : T, reserve : Int) : Long = {
    data.put(next_id, node)
    val result = next_id
    next_id += 1
    next_id
  }
  
  def write(pointer : Long, node : T) : Unit = {
    data.put(pointer, node)
  }
  
  def read(pointer : Long) : T =
    data.get(pointer)
    
  def commit() = {}
}