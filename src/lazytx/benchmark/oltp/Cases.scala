package lazytx.benchmark.oltp

import java.lang.management.ManagementFactory
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer

trait SimpleBenchmark {
  def workload(dbsize : Int) : () => Unit
}

trait SimpleAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) : () => Unit
}

trait RandomAccessBenchmark extends SimpleAccessBenchmark {
  // Transactions should access data uniform randomly
}

trait SequentialAccessBenchmark extends SimpleAccessBenchmark {
  // Threads access data sequentially in one block
}

trait OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) : () => Unit
}

object GenericBenchmark {
  def run_gc(threads : Int, time : Int, benchmark : GenericBenchmark, dbsize : Int, updateCount : Int, keyReadCount : Int, valueReadCount : Int, constraintCount : Int) = {
    getGarbageCollectionTime()
    val start = System.nanoTime()
    val before = getGarbageCollectionTime
    val r = Benchmark.benchmark(threads, time, benchmark.workload(dbsize, updateCount, keyReadCount, valueReadCount, constraintCount))
    val after = getGarbageCollectionTime
    val end = System.nanoTime()
    
    val elapsed = (end - start) / 1000000000.0
    val gc = (after - before) / 1000.0 
    
    r / (elapsed - gc) * elapsed
  }
  
  def getGarbageCollectionTime() : Long = {
    var collectionTime = 0L;
    for (garbageCollectorMXBean <- ManagementFactory.getGarbageCollectorMXBeans().toList) {
        collectionTime += garbageCollectorMXBean.getCollectionTime();
    }
    collectionTime
  }
}

trait GenericBenchmark {
  def workload(dbsize : Int, updateCount : Int, keyReadCount : Int, valueReadCount : Int, constraintCount : Int) : () => Unit
}