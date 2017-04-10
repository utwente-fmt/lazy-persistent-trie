package lazytx.benchmark.batch

import lazytx.State
import lazytrie.Map
import lazytrie.test.Util
import java.util.concurrent.ThreadLocalRandom
import java.lang.management.ManagementFactory
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer
import laziness.Lazy
import lazytx.MapDiff

object Main {
  def main(args : Array[String]) : Unit = {
    /*
    val warmup = 3.0
    val time = args(0).toDouble
    val throughput = args(1).toInt
    val threads = args(2).toInt
    val queueSize = args(3).toInt
    
    val txsize = 10
    
    while(true) {
      var rs = ArrayBuffer[ArrayBuffer[Double]]()
      
      for(dbsize <- Array(100000)) { // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384
        println("Size " + dbsize * txsize)
        val wl = new RandomAccessWorkload(dbsize * txsize, txsize)
      
        println("Threads " + threads + " / Queuesize " + queueSize)
        val (elapsed, gc, latency, wr) = new LatencyBenchmark(wl, warmup, time, throughput, threads, queueSize).run
        rs += LatencyBenchmark.report(elapsed, gc, latency, wr)
        println
      }
      
      rs.transpose.foreach(x => { x.foreach(v => print(v + " ")); println })
    }
      * 
      */
  }
}