package lazytx.benchmark

import java.util.Arrays
import java.util.Comparator
import java.util.HashSet
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable.ArrayBuffer

import lazytrie.ComposableMap.toComposableMap
import lazytrie.Map
import lazytrie.test.Util
import lazytx.benchmark.oltp.Benchmark

/**
 * TODO: make sure n different partitions are selected
 */

case class MTuple3[A,B](var a : A, var b : B)

class Tuple2Comparator[A] extends Comparator[(Int,A)] {
  override def compare(a : (Int,A), b : (Int,A)) = {
    a._1 - b._1
  }
}

case class Locked(var isLocked : Boolean)

object Partitioning {
  val time = 5
  val padding = 16
  var maxBuffer = 128
  
  def main(args : Array[String]) : Unit = {
    
    val dbsize = 1048576
    
    val threads = args(0).toInt
    val operationsTx = args(1).toInt
    maxBuffer = args(2).toInt
    
    while(true) {
      for (partitionsTx <- Array(1, 2, 4, 8)) {
        if(partitionsTx <= operationsTx) {
          println("# Partitions per tx: "+ partitionsTx)
          for (i <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)) {
            if(partitionsTx <= i) {
              val t1 = Benchmark.benchmark(1, time * 1000, workload(i, dbsize / i, partitionsTx, operationsTx))
              val tn = Benchmark.benchmark(threads, time * 1000, workload(i, dbsize / i, partitionsTx, operationsTx))
              println(i, t1.toLong, tn.toLong, tn.toFloat / t1.toFloat)
            }
          }
        }
      }
    }
  }
  
  def workload(partitionCount: Int, partitionSize: Int, txPartitionCount: Int, txOperationCount: Int) = {
    val db = Array.ofDim[Map[Int, Int]](partitionCount * padding)
    val forceBuffers = Array.ofDim[ArrayBuffer[Map[Int, Option[Int] => Option[Int]]]](partitionCount * padding)
    for (i <- 0 until partitionCount) {
      db(i * padding) = Util.createIntMap(partitionSize)
      forceBuffers(i) = new ArrayBuffer(maxBuffer)
    }
      
    val context = db(0).context
    val emptyDiff = db(0).emptyMap[Option[Int] => Option[Int]]
    val f = (x : Option[Int]) => x.map(_ + 1)
    val cmp = new Tuple2Comparator[Map[Int, Option[Int] => Option[Int]]]
    
    () => {
      val random = ThreadLocalRandom.current()    
      val partitions = Array.ofDim[(Int, Map[Int, Option[Int] => Option[Int]])](txPartitionCount)

      val ps = new HashSet[Int]()
      
      var i = 0
      while(i < txPartitionCount) {
        var p = 0
        do {
          p = random.nextInt(partitionCount)
        } while(ps.contains(p))       
        ps.add(p)
        
        val diff = emptyDiff.empty
        diff(random.nextInt(partitionSize)) = f
        partitions(i) = (random.nextInt(partitionCount), diff)
        i += 1
      }
      
      i = txPartitionCount
      while(i < txOperationCount) {
        partitions(random.nextInt(txPartitionCount))._2.composeInPlace(random.nextInt(partitionSize), f)
        i += 1
      }
      
      Arrays.sort(partitions, cmp)
      doUpdate(partitions, partitions.length - 1)
      
      // TODO: merge this with commit?
      partitions.foreach(p => {
        val pid = p._1
        var buffer : ArrayBuffer[Map[Int, Option[Int] => Option[Int]]] = null
        
        val fb = forceBuffers(pid)
        fb.synchronized {
          fb += p._2
          if(fb.size >= maxBuffer) {
            buffer = fb.clone()
            fb.clear()
          }
        }
        
        if(buffer != null) {
          val s = db(p._1 * padding)
          buffer.foreach(s.force(_))
        }
      })
    }
    
    def doUpdate(partitions : Array[(Int, Map[Int, Option[Int] => Option[Int]])], id : Int) : Unit = {   
      val pid = partitions(id)._1
      
      db(pid * padding).synchronized {
        db(pid * padding) = db(pid * padding).update(partitions(id)._2)
        
        if(id > 0) {
          doUpdate(partitions, id - 1)
        }
      }
    }
  }
}