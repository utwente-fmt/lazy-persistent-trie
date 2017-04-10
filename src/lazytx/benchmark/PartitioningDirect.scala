package lazytx.benchmark

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.locks.ReentrantLock

import lazytrie.ComposableMap
import lazytrie.ComposableMap.toComposableMap
import lazytrie.Map
import lazytrie.test.Util
import lazytx.benchmark.oltp.Benchmark

/**
 * TODO: make sure n different partitions are selected
 */

object PartitioningDirect {
  val threads = 8
  val warmup = 5
  val time = 5
  
  def main(args : Array[String]) : Unit = {
    Benchmark.benchmark(threads, warmup * 1000, workload(1, 1048576, 5, 10))
    
    val operationsTx = 10
    val partitionsElem = 1024
    
    for (partitionsTx <- Array(1, 2, 4, 6, 8, 10)) {
      println("# Partitions per tx: "+ partitionsTx)
      for (i <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)) {
        val result = Benchmark.benchmark(threads, time * 1000, workload(i, partitionsElem, partitionsTx, operationsTx))
        println("("+i+","+(result.toLong).toDouble/1000000+")")
      }
    }
  }
  
  def workload(partitionCount: Int, partitionSize: Int, txPartitionCount: Int, txOperationCount: Int) = {
    val db = Array.ofDim[Map[Int, Int]](partitionCount)
    for (i <- 0 until partitionCount) {
      db(i) = Util.createIntMap(partitionSize)
    }
    
    () => {
      val random = ThreadLocalRandom.current()    
      
      // Select partitions and sort
      var partitions = Array.ofDim[Int](txPartitionCount)
      var i = 0
      while(i < txPartitionCount) {
        partitions(i) = random.nextInt(partitionCount)
        i += 1
      }      
      Arrays.sort(partitions)
      
      // Queue operations
      val operations = Array.ofDim[Map[Int,Int] => Map[Int,Int]](txPartitionCount)
      for(i <- 0 until txPartitionCount) {
        val k = random.nextInt()
        operations(i) = (m => m.modify(k, _ + 1))
      }
      
      for(i <- txPartitionCount until txOperationCount) {
        val p = random.nextInt(txPartitionCount)
        val k = random.nextInt()
        operations(p) = ((m : Map[Int,Int]) => m.modify(k, _ + 1)) compose operations(p)
      }
      
      // Lock and perform operations
      val forces = Array.ofDim[() => Unit](txOperationCount)
      
      println(partitions.mkString(" "))
      
      doUpdate(partitions, partitions.length - 1, () => {
        for(i <- 0 to txPartitionCount) {
          val pid = partitions(i)
          db(pid) = operations(i)(db(pid))
        }
      })
      
      //partitions.foreach(p => locks(p).unlock())
      
      forces.foreach(_())
    }
    
    def doUpdate(lock : Array[Int], id : Int, c : () => Unit) : Unit = {
      if(id >= 0) {
        db(lock(id)).synchronized {
          doUpdate(lock, id - 1, c)
        }
      } else {
        c()
      }
    }
  }
}