package lazytx.benchmark.oltp

import java.lang.management.ManagementFactory

import scala.collection.JavaConversions.asScalaBuffer

object Main {
  val branchWidth = 5
  val leafWidth = 4
  
  val time = 5000
  //val dbsize = 100000
  val threads = 1
  val TXSIZES = Array(1, 4, 16, 64, 256, 1024, 4096, 8192)
  
  def main(args : Array[String]) : Unit = {
    /*
    val time = 5000
    var dbsize = 1048576
    
    val updateCount = 8
    val keyReadCount = 2
    val valueReadCount = 8
    val constraintCount = 4
    
    var approaches = Array(
      new TwoPhaseLockingGenericBenchmark()
    )
    
    for(approach <- approaches) {
      println(approach.getClass.getName())
      for(threads <- Array(64, 1)) {
        print(threads + " threads: ")
        for(dbsize <- Array(1048576, 524288, 262144, 131072, 65536, 32768, 16384, 8192, 4096, 2048, 1024, 512, 256, 128, 64, 32, 16)) {
          val throughput = GenericBenchmark.run_gc(threads, time, approach, dbsize, updateCount, keyReadCount, valueReadCount, constraintCount)
          print(throughput + " ")  
        }
        println
      }
    }
    */
    
    while(true) {
      print("blocking: ")
      for(threads <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256)) {
        print(simple_gc(new BlockingStateUpdate(), threads, 5000, 32).toLong)
        print(" ")
      }
      println
      
      print("non-blocking: ")
      for(threads <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256)) {
        print(simple_gc(new NonBlockingStateUpdate(), threads, 5000, 32).toLong)
        print(" ")
      }
      println
    }
  }
  
  def simple(benchmark : SimpleBenchmark, threads : Int, time : Int, dbsize : Int) = {
    Benchmark.benchmark(threads, time, benchmark.workload(dbsize))
  }
  
  def simple_gc(benchmark : SimpleBenchmark, threads : Int, time : Int, dbsize : Int) = {
    
    getGarbageCollectionTime()
    val start = System.nanoTime()
    val before = getGarbageCollectionTime
    val r = Benchmark.benchmark(threads, time, benchmark.workload(dbsize))
    val after = getGarbageCollectionTime
    val end = System.nanoTime()
    
    val elapsed = (end - start) / 1000000000.0
    val gc = (after - before) / 1000.0 
    
    r / (elapsed - gc) * elapsed
  }
  
  def throughput(benchmark : SimpleAccessBenchmark, threads : Int, time : Int, dbsize : Int, writeRatio : Double) = {
    println(benchmark.getClass.getName())
    
    for(i <- 1 to 10) {
      Benchmark.benchmark(8, 1000, benchmark.workload(dbsize, 256, 0.5))
    }
    
    val start = System.nanoTime()
    getGarbageCollectionTime()
    val before = getGarbageCollectionTime
    for(txsize <- TXSIZES) {
      val txs = Benchmark.benchmark(threads, time, benchmark.workload(dbsize, txsize, writeRatio))
      print((txs * txsize).toLong + " ")
    }
    val after = getGarbageCollectionTime
    val end = System.nanoTime()
    
    val elapsed = (end - start) / 1000000000.0
    val gc = (after - before) / 1000.0 
    
    println()
    //println(elapsed, gc, gc / elapsed)
  }
  
  def lazyRead(benchmark : OptimisticBenchmark, threads : Int, time : Int, dbsize : Int, readsPerWrite : Int) = {
    println(benchmark.getClass.getName())
    
    for(i <- 1 to 10) {
      Benchmark.benchmark(8, 1000, benchmark.workload(dbsize, 1, readsPerWrite))
    }
    
    for(txsize <- TXSIZES) {
      val txs = Benchmark.benchmark(threads, time, benchmark.workload(dbsize, readsPerWrite, txsize))
      print((txs * txsize * (readsPerWrite + 1)).toLong + " ") 
    }
    println
  }
  
  def optimistic(benchmark : OptimisticBenchmark, threads : Int, time : Int, dbsize : Int, readsPerWrite : Int) = {
    println(benchmark.getClass.getName())
    
    for(i <- 1 to 10) {
      Benchmark.benchmark(8, 1000, benchmark.workload(dbsize, 1000, 1000))
    }
    
    for(txsize <- TXSIZES) {
      val txs = Benchmark.benchmark(threads, time, benchmark.workload(dbsize, readsPerWrite * txsize, txsize))
      print((txs * txsize * (readsPerWrite + 1)).toLong + " ")
    }
    println
  }
  
  def getGarbageCollectionTime() : Long = {
    var collectionTime = 0L;
    for (garbageCollectorMXBean <- ManagementFactory.getGarbageCollectorMXBeans().toList) {
        collectionTime += garbageCollectorMXBean.getCollectionTime();
    }
    collectionTime
  }
}