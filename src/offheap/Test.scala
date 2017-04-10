package offheap

import scala.collection.mutable.Buffer
import laziness.Lazy
import laziness.LockingThunk

abstract class TestNode extends Node[TestNode] { }

case class StringNode(value : String) extends TestNode {
  def children : Iterable[Long] = List()
  def move(newChildren : Iterable[Long]) = this
}

case class LazyNode(f : () => TestNode) extends TestNode {
  def children : Iterable[Long] = List()
  def move(newChildren : Iterable[Long]) = this
}


object Test {
  def main(args : Array[String]) = {
    val heap = new OnDiskHeap[TestNode]("database")
    
    val ptr = heap.append(new LazyNode(() => new StringNode("it works!")) )
    val l = heap.read(ptr).asInstanceOf[LazyNode]
    heap.write(ptr, l.f())
    println(heap.read(ptr))
    
    // Goal: we want to talk to an abstraction that understands laziness:
      // write(thunk) : pointer
      // replace(pointer, value)
      // read(pointer) : value
    // implementation guarantees storage and sharing, should provide caching of reads, preferably batches writes, and can be garbage collected
    // note: for compacting tries we need to re-write nodes in-place
    
    // idealy we compact and garbage collect lazily
      // piggyback with other operations + use idle time
    
    // the implementation behind this abstraction doesnt matter much for a prototype
    
    // how do we implement sharing correctly?
      // correctness means reference equivalent results
        // we have to ensure concurrent readers from disk get the same node
        // we have to ensure, when two readers have the same node, they evaluate to the same result
      // options:
      // a) evaluate on read, assuming reads are not concurrent (but we definitely want concurrent reads)
      // b) lock something unique identifying the thunk (e.g. its hash)
      // c) ensure uniqueness of objects using caching: just read the object, but after reading, ensure that only a single object is in the cache
      //  problem is that the consumer could hold onto the object while it is already purged from the cache, after which other threads could receive a duplicate
      // d) evaluate on cache: read the object through a cache, while the cache evaluates the object: this guarantees absolute correctness
        // can also automatically write the result back
        // once written back it may be purged from the cache
        // we only need caching while evaluating nodes 
    
    /*
    lazy_node = read(ptr)
    val result = lazy_node.evaluate()
    rewrite(ptr, result)
    */
    
    /*
    val ptrs = Buffer[Long]()
    
    for(i <- 1 to 1000000) {
      ptrs += heap.write(new StringNode("data " + i))
    }
   
    for(ptr <- ptrs) {
      println(heap.read(ptr))
    }
    */
  }
}