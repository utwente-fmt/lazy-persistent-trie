package lazytx.benchmark.batch.tpcc

import lazytx.State
import lazytrie.Map
import lazytrie.test.Util
import java.util.concurrent.ThreadLocalRandom
import java.lang.management.ManagementFactory
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer
import laziness.Lazy
import lazytx.MapDiff
import lazytx.benchmark.batch.WorkerReport
import lazytx.benchmark.batch.LatencyBenchmark
import lazytx.benchmark.tpcc.Benchmark
import lazytx.benchmark.tpcc.LazyTpcc
import lazytx.benchmark.tpcc.Database
import scala.collection.mutable.Buffer
import lazytx.benchmark.batch.Worker

object Main {
  def main(args : Array[String]) : Unit = {
    val warmup = 3.0
    val time = args(0).toDouble
    val throughput = args(1).toInt
    val pinThreads = args(2).toBoolean
    
    if(args.length > 5) {
      Benchmark.DELIVERY = args(5).toInt
      Benchmark.ORDER_STATUS = args(6).toInt
      Benchmark.STOCK_LEVEL = args(7).toInt
      Benchmark.PAYMENT = args(8).toInt
    }
    
    for(queueSize <- Array(1, 1, 2, 4, 8, 16, 32, 64, 128, 256)) {
      val gc_1 = Buffer[Long]()
      val gc_64 = Buffer[Long]()
      val nogc_1 = Buffer[Long]()
      val nogc_64 = Buffer[Long]()
      
      for(warehouses <- Array(1, 2, 4, 8, 16, 32, 64, 128)) {
        var rs = ArrayBuffer[ArrayBuffer[Double]]()
        
        val state = new State(LazyTpcc.generate(warehouses))
        DatabaseDiff.init(state.get)
        
        val (tps_1, tps_1_nogc) = run(state, warehouses, warmup, time, throughput, queueSize, 1, pinThreads)
        val (tps_64, tps_64_nogc) = run(state, warehouses, warmup, time, throughput, queueSize, 64, pinThreads)

        gc_1 += tps_1.toLong
        nogc_1 += tps_1_nogc.toLong
        
        gc_64 += tps_64.toLong
        nogc_64 += tps_64_nogc.toLong
        
        println(tps_1, tps_1_nogc, tps_64, tps_64_nogc, tps_64_nogc.toFloat / tps_1_nogc)
      }
      
      println("Without garbage collection:")
      println(" 1 thread:   " + nogc_1.mkString(" "))
      println(" 64 threads: " + nogc_64.mkString(" "))
      println(" speedup:    " + nogc_1.zip(nogc_64).map({ case (a,b) => b.toFloat/a}).mkString(" "))
      println("With garbage collection:")
      println(" 1 thread:   " + gc_1.mkString(" "))
      println(" 64 threads: " + gc_64.mkString(" "))
      println(" speedup:    " + gc_1.zip(gc_64).map({ case (a,b) => b.toFloat/a}).mkString(" "))
    }
  }
  
  def average(runs : Int, state : State[Database], warehouses : Int, warmup : Double, time : Double, throughput : Int, queueSize : Int, threads : Int, pinThreads : Boolean) : (Double, Double) = {
    var with_gc = Buffer[Double]()
    var without_gc = Buffer[Double]()
    for(i <- 1 to runs) {
      var r = run(state, warehouses, warmup, time, throughput, queueSize, threads, pinThreads)
      with_gc += r._1
      without_gc += r._2
    }
    
    (
      with_gc.sortBy(x => x).drop(2).dropRight(2).sum / (with_gc.length - 4),
      without_gc.sortBy(x => x).drop(2).dropRight(2).sum / (without_gc.length - 4)
    )
  }
  
  def run(state : State[Database], warehouses : Int, warmup : Double, time : Double, throughput : Int, queueSize : Int, threads : Int, pinThreads : Boolean) = {
    val wl = Array.ofDim[LazyTpccWorkload](threads)
          
    if(pinThreads) {
      if(warehouses < threads) {
        val threadsPerWarehouse = threads / warehouses
        for(i <- 0 until threads) {
          val w_low = 1 + (i / threadsPerWarehouse) 
          wl(i) = new LazyTpccWorkload(state, warehouses, w_low, w_low)
        }
      } else {
        val warehousesPerThread = warehouses / threads
        for(i <- 0 until threads) {
          val w_low = 1 + i * warehousesPerThread
          wl(i) = new LazyTpccWorkload(state, warehouses, w_low, w_low + warehousesPerThread - 1)
        }
      }
    } else {
      for(i <- 0 until threads) {
        wl(i) = new LazyTpccWorkload(state, warehouses, 1, warehouses)
      }
    }
    
    val bench = new LatencyBenchmark(state, wl, warmup, time, throughput, queueSize)
    val (elapsed, gc, latency, wr) = bench.run
    
    val ts = latency.reduce(_ + _)
    val tpc = ts / elapsed
    val tpc_nogc = ts / (elapsed - gc)
    
    (tpc, tpc_nogc)
  }
}