package offheap

import java.util.HashMap
import java.io.RandomAccessFile
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.nio.channels.Channels

trait Node[T] extends Serializable {
  def children : Iterable[Long]
  def move(newChildren : Iterable[Long]) : T
}

trait Heap[T <: Node[T]] {
  // Append a node, returning a pointer to this data for reading
  def append(node : T, reserve : Int = 0) : Long
 
  // Overwrite data at a certain pointer
  def write(pointer : Long, node : T) : Unit
  
  // Read data from disk by its pointer
  def read(pointer : Long) : T
  
  // Force writes to disk, guarantees succesful reads
  def commit() : Unit
  
  // Copy reachable nodes from another heap to this heap
  def copy(root : T, from : Heap[T]) : Long =
    append(root.move(root.children.map(ptr => copy(read(ptr), from))))
}