package lazytx.benchmark.batch

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport
import scala.collection.mutable.ArrayBuffer
import lazytx.State
import java.lang.management.ManagementFactory
import scala.collection.JavaConversions.asScalaBuffer
import lazytx.Diff

abstract class Workload[T >: Null, D <: Diff[T]] {
  def emptyDiff : D
  def makeTask : Task[T,D]
}

object LatencyBenchmark {
  val scale = Array.ofDim[Double](3000)
  scale(1500) = 1.0
  for(i <- 1499 to 0 by -1)
    scale(i) = scale(i+1) / 1.01
  for(i <- 1501 to 2999)
    scale(i) = scale(i-1) * 1.01

    /*
  def speedup[T >: Null, D <: Diff[T]](wl : Workload[T,D], warmup : Double, time : Double, throughput : Int, threads : Int, queueSize : Int) = {
    var rs = ArrayBuffer[ArrayBuffer[Double]]()
    for(
    val bench = new LatencyBenchmark(wl, warmup, time, throughput, threads, queueSize)
    val (elapsed, gc, latency, wr) = bench.run
    rs += report(elapsed, gc, latency, wr)
    println
  }
  */
    
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
    println(" Waiting:    " + "%.3f".format((wr.waiting / wr.totalTime) * 100) + "%")
    println(" Generating: " + "%.3f".format((wr.generating / wr.totalTime) * 100) + "%")
    println(" Queueing:   " + "%.3f".format((wr.queueing / wr.totalTime) * 100) + "%")
    println(" Committing: " + "%.3f".format((wr.committing / wr.totalTime) * 100) + "%")
    println(" Notifying:  " + "%.3f".format((wr.notifying / wr.totalTime) * 100) + "%")
    println(" Forcing:    " + "%.3f".format((wr.forcing / wr.totalTime) * 100) + "%")
    
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

class LatencyBenchmark[T >: Null, D <: Diff[T]](state : State[T], workload : Array[_ <: Workload[T,D]], warmup : Double, time : Double, throughput : Int, queueSize : Int) {
  def run : (Double, Double, List[Long], WorkerReport) = {
    val workers = Array.ofDim[Worker[T,D]](workload.size) 
    val generators = Array.ofDim[Generator[T,D]](workload.size)
    val reporters = Array.ofDim[LatencyReporter](workload.size)
    val started = new AtomicBoolean(false)
    val stopped = new AtomicBoolean(false)
    
    for(i <- 0 until workload.size) {
      val w = workload(i)
       
      reporters(i) = new LatencyReporter(started, stopped)
      generators(i) = new Generator[T,D](() => w.makeTask, throughput / workload.size, queueSize, reporters(i))
      workers(i) = new Worker(generators(i), () => w.emptyDiff, state, started, stopped)
    }

    // Start workers and workload
    workers.foreach(_.start)
    
    Thread.sleep((warmup * 1000).toLong)
    
    // Signal start
    started.set(true)
    
    getGarbageCollectionTime()
    val start = System.nanoTime()
    val before = getGarbageCollectionTime
    var after = 0L
    var end = 0L
    
    // Wait
    while(!stopped.get) {
      Thread.sleep(1000L)
      
      after = getGarbageCollectionTime
      end = System.nanoTime()
    
      val passed_real = (end - start) / 1000000000.0
      val passed_gc = (after - before) / 1000.0
      
      val passed_work = passed_real - passed_gc
      
      if(passed_work >= time) {
        // Signal stop
        stopped.set(true)
      }
    }
    
    // Join threads
    workers.foreach(_.join)
    
    // Gather and merge results
    val elapsed = (end - start) / 1000000000.0
    val gc = (after - before) / 1000.0 
    val latency = reporters.map(_.report).reduce((a,b) => a.zip(b).map({ case (x,y) => x + y })).toList
    val workerReport = workers.map(_.report).reduce((a,b) => a.merge(b))
    
    (elapsed, gc, latency, workerReport)
  }
  
  def getGarbageCollectionTime() : Long = {
    var collectionTime = 0L;
    for (garbageCollectorMXBean <- ManagementFactory.getGarbageCollectorMXBeans().toList) {
        collectionTime += garbageCollectorMXBean.getCollectionTime();
    }
    collectionTime
  }
}

class LatencyReporter(started : AtomicBoolean, stopped : AtomicBoolean) {
  val bins = Array.ofDim[Long](3000)
  for(i <- 0 to bins.length - 1)
    bins(i) = 0
    
  def report(latency : Double) {
    if(started.get && !stopped.get) {
      val log2 = Math.log(latency) / Math.log(1.01)
      val bin = Math.max(log2.ceil.toInt + 1500, 0)  
      
      bins(bin) += 1
    }
  }
  
  def report = bins
}

class LatencyTask[T >: Null, D <: Diff[T]](task : Task[T,D], reporter : LatencyReporter) extends Task[T,D] {
  var start = System.nanoTime()
  
  def enqueue(diff : D) = task.enqueue(diff)
  def result(state : T) = {
    task.result(state)
    reporter.report((System.nanoTime - start) / 1000000000.0)
  }
}