package offheap

import java.io.ObjectInputStream
import java.nio.channels.Channels
import java.io.RandomAccessFile
import java.io.ObjectOutputStream

class OnDiskHeap[T <: Node[T]](filename : String) extends Heap[T] {
  val file = new RandomAccessFile(filename, "rw")
  
  def append(node : T, reserve : Int = 1) : Long = {
    file.seek(file.length())
    val pointer = file.getFilePointer
    val oos = new ObjectOutputStream(Channels.newOutputStream(file.getChannel))
    oos.writeObject(node)
    oos.flush()
    var current = file.getFilePointer()
    file.seek(file.length());
    while(current < reserve) {
      file.writeByte(0)
      current += 1
    }
    pointer
  }
  
  def write(pointer : Long, node : T) : Unit = {
    file.seek(pointer)
    val oos = new ObjectOutputStream(Channels.newOutputStream(file.getChannel))
    oos.writeObject(node)
    oos.flush()
  }
  
  def read(pointer : Long) : T = {
    file.seek(pointer)
    val ois = new ObjectInputStream(Channels.newInputStream(file.getChannel))
    val value = ois.readObject
    value.asInstanceOf[T]
  }
  
  def commit() = {
    file.getFD.sync()
  }
}