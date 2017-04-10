package offheap

import java.util.LinkedHashMap
import java.util.HashMap

/*
class CacheHeap[T <: Node[T]](readSize : Int, writeSize : Int, source : Heap[T]) extends Heap[T] {
  val writes = new HashMap[Long, (T, Int)]
  val reads = new LinkedHashMap[Long, T]
  var pointer : Int = source.tell
  
  def append(node : T, reserve : Int) : Long = {
    val ptr = pointer
    writes.put(pointer, (node, reserve))
    pointer += reserve
    ptr
  }
  
  def write(pointer : Long, node : T) : Unit = {
    data.put(pointer, node)
  }
  
  def read(pointer : Long) : T = {
    // we assume that values are immutable
    var result = writes.get(pointer)._1
    if(result == null) {
      result = reads.get(pointer)
      if(result == null) {
        result = source.read(pointer)
        reads.put(pointer, result)
      }
    }
    result
  }
  
  def commit() = {    
    for((ptr, write) <- writes) {
      source.append(write._1, write._2)
    }
  }
  
  // Only write reachable elements
    // Note: other elements may still be needed by transactions that are in progress, are these reachable from the root?
  def commit(root : Node) = {
    
  }
}
*/