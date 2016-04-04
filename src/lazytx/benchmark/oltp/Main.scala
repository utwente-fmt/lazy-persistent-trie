package lazytx.benchmark.oltp

import java.lang.management.ManagementFactory
import scala.collection.JavaConversions._

object Main {
  val branchWidth = 5
  val leafWidth = 4
  
  val time = 120000
  val dbsize = 1000000
  val threads = 8
  val TXSIZES = Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288)
  
  def main(args : Array[String]) : Unit = {
    // Lazy
    throughput(new LazyRandomAccessBenchmark(0, branchWidth, true), threads, time, dbsize, 0.1)
    throughput(new LazyRandomAccessBenchmark(leafWidth, branchWidth, true), threads, time, dbsize, 0.1)
    throughput(new LazyRandomAccessBenchmark(0, branchWidth, false), threads, time, dbsize, 0.1)
    
    throughput(new LazyRandomAccessBenchmark(0, branchWidth, true), threads, time, dbsize, 0.9)
    throughput(new LazyRandomAccessBenchmark(leafWidth, branchWidth, true), threads, time, dbsize, 0.9)
    throughput(new LazyRandomAccessBenchmark(0, branchWidth, false), threads, time, dbsize, 0.9)
    
    throughput(new LazySequentialAccessBenchmark(0, branchWidth, true), threads, time, dbsize, 0.1)
    throughput(new LazySequentialAccessBenchmark(leafWidth, branchWidth, true), threads, time, dbsize, 0.1)
    throughput(new LazySequentialAccessBenchmark(0, branchWidth, false), threads, time, dbsize, 0.1)
    
    throughput(new LazySequentialAccessBenchmark(0, branchWidth, true), threads, time, dbsize, 0.9)
    throughput(new LazySequentialAccessBenchmark(leafWidth, branchWidth, true), threads, time, dbsize, 0.9)
    throughput(new LazySequentialAccessBenchmark(0, branchWidth, false), threads, time, dbsize, 0.9)
    
    lazyRead(new LazyRandomReadBenchmark(0, branchWidth, true), threads, time, dbsize, 1)
    lazyRead(new LazyRandomReadBenchmark(leafWidth, branchWidth, true), threads, time, dbsize, 1)
    
    lazyRead(new LazyRandomReadBenchmark(0, branchWidth, true), threads, time, dbsize, 10)
    lazyRead(new LazyRandomReadBenchmark(leafWidth, branchWidth, true), threads, time, dbsize, 10)
    
    optimistic(new LazyOptimisticBenchmark(1, 0, branchWidth, true), threads, time, dbsize, 1)
    optimistic(new LazyOptimisticBenchmark(1, leafWidth, branchWidth, true), threads, time, dbsize, 1)
    optimistic(new LazyOptimisticBenchmark(1, 0, branchWidth, false), threads, time, dbsize, 1)
    
    optimistic(new LazyOptimisticBenchmark(1, 0, branchWidth, true), threads, time, dbsize, 10)
    optimistic(new LazyOptimisticBenchmark(1, leafWidth, branchWidth, true), threads, time, dbsize, 10)
    optimistic(new LazyOptimisticBenchmark(1, 0, branchWidth, false), threads, time, dbsize, 10)
    
    // ScalaSTM
    throughput(new StmRandomAccessBenchmark, threads, time, dbsize, 0.1)
    throughput(new StmRandomAccessBenchmark, threads, time, dbsize, 0.9)
    throughput(new StmSequentialAccessBenchmark, threads, time, dbsize, 0.1)
    throughput(new StmSequentialAccessBenchmark, threads, time, dbsize, 0.9)
    lazyRead(new StmRandomReadWriteBenchmark(), threads, time, dbsize, 1)
    lazyRead(new StmRandomReadWriteBenchmark(), threads, time, dbsize, 10)
    optimistic(new StmOptimisticBenchmark(), threads, time, dbsize, 1)
    optimistic(new StmOptimisticBenchmark(), threads, time, dbsize, 10)
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
      print((txs * txsize * (readsPerWrite + 1)).toLong + " ms")
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