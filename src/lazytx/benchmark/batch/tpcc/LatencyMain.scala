package lazytx.benchmark.batch.tpcc

import lazytx.benchmark.tpcc.LazyTpcc
import lazytx.benchmark.batch.LatencyBenchmark
import lazytx.benchmark.batch.WorkerReport
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import lazytx.State
import lazytx.benchmark.tpcc.Database
import lazytx.benchmark.tpcc.Benchmark

object LatencyMain {
  def main(args : Array[String]) : Unit = {
    val warmup = 3.0
    val time = args(0).toDouble
    val throughput = args(1).toInt
    val threads = args(2).toInt
    val warehouses = args(3).toInt
    val pinThreads = args(4).toBoolean
    
    if(args.length > 5) {
      Benchmark.DELIVERY = args(5).toInt
      Benchmark.ORDER_STATUS = args(6).toInt
      Benchmark.STOCK_LEVEL = args(7).toInt
      Benchmark.PAYMENT = args(8).toInt
    }
    
    val state = new State(LazyTpcc.generate(warehouses))
    while(true) {
      println(warehouses + " warehouses")
      var results = ArrayBuffer[ArrayBuffer[Double]]()
      for(queueSize <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256)) {
        var rs = ArrayBuffer[ArrayBuffer[Double]]()
        DatabaseDiff.init(state.get)
        results += run(state, warehouses, warmup, time, throughput, queueSize, threads, pinThreads)
      }
      println(results.transpose.map(_.mkString(" ")).mkString("\n"))
      println()
    }
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
    
    report(elapsed, gc, latency, wr)
  }

  val scale = Array.ofDim[Double](3000)
    scale(1500) = 1.0
    for(i <- 1499 to 0 by -1)
      scale(i) = scale(i+1) / 1.01
    for(i <- 1501 to 2999)
      scale(i) = scale(i-1) * 1.01

  def report(elapsed : Double, gc : Double, latency : List[Long], wr : WorkerReport) : ArrayBuffer[Double] = {
    val total = latency.reduce(_ + _)
      
    var slow = -1.0
    var p999 = -1.0
    var p99 = -1.0
    var p9 = -1.0
    var p2 = -1.0
    
    var acc = 0L
    var i = 0
    for(m <- latency) {
      acc += m
      
      if(p999 < 0 && acc / total.toDouble > 0.999) {
        p999 = scale(i)
      }
      
      if(p99 < 0 && acc / total.toDouble > 0.99) {
        p99 = scale(i)
      }
      
      if(p9 < 0 && acc / total.toDouble > 0.9) {
        p9 = scale(i)
      }
      
      if(p2 < 0 && acc / total.toDouble > 0.5) {
        p2 = scale(i)
      }
      
      if(m > 0)
        slow = scale(i)
      
      i += 1
    }
    
    
    /*
    println("Throughput report:")
    println(" Including GC: " + (total / elapsed).toLong + " tx/s")
    println(" Excluding GC: " + (total / (elapsed - gc)).toLong + " tx/s")
    
    println("Latency report:")
    println(" slow:  " + "%.6f".format(slow * 1000) + " ms")
    println(" 1/999: " + "%.6f".format(p999 * 1000) + " ms")
    println(" 1/99:  " + "%.6f".format(p99 * 1000) + " ms")
    println(" 1/9:   " + "%.6f".format(p9 * 1000) + " ms")
    println(" 1/2:   " + "%.6f".format(p2 * 1000) + " ms")
    
    println("Worker report:")
    println(" Average concurrent transactions: " + "%.3f".format(wr.concurrency))
    println(" Waiting:    " + "%.3f".format((wr.waiting / wr.totalTime) * 100) + "%")
    println(" Generating: " + "%.3f".format((wr.generating / wr.totalTime) * 100) + "%")
    println(" Queueing:   " + "%.3f".format((wr.queueing / wr.totalTime) * 100) + "%")
    println(" Committing: " + "%.3f".format((wr.committing / wr.totalTime) * 100) + "%")
    println(" Notifying:  " + "%.3f".format((wr.notifying / wr.totalTime) * 100) + "%")
    println(" Forcing:    " + "%.3f".format((wr.forcing / wr.totalTime) * 100) + "%")
    */
    
    ArrayBuffer(
        (total / elapsed).toLong,
        (total / (elapsed - gc)).toLong,
        slow * 1000,
        p999 * 1000,
        p99 * 1000,
        p9 * 1000,
        p2 * 1000,
        wr.waiting / wr.totalTime,
        wr.generating / wr.totalTime,
        wr.queueing / wr.totalTime,
        wr.committing / wr.totalTime,
        wr.notifying / wr.totalTime,
        wr.forcing / wr.totalTime
    )
  }
}