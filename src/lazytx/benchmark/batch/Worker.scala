package lazytx.benchmark.batch

import java.util.concurrent.locks.LockSupport
import lazytx.State
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicBoolean
import lazytx.Diff
import java.lang.management.ManagementFactory
import scala.collection.JavaConversions.asScalaBuffer

class Queue[T](capacity : Int) {
  var buffer = new ArrayBuffer[T]
  
  def swap(other : ArrayBuffer[T]) = {
    this.synchronized {
      val t = buffer
      buffer = other
      t
    }
  }
  
  // Returns number of tasks dropped
  def enqueue(tasks : ArrayBuffer[T]) : Int = {
    this.synchronized {
      val remainingCapacity = capacity - buffer.length
      
      if(remainingCapacity < tasks.length) {
        var i = 0
        while(i < remainingCapacity) {
          buffer += tasks(i)
          i += 1
        }
        
        tasks.length - remainingCapacity
      } else {
        buffer ++= tasks
        0
      }
    }
  }
}

class Generator[T >: Null, D <: Diff[T]](generate : () => Task[T,D], throughput : Int, capacity : Int, reporter : LatencyReporter) {
  var startTime = 0L
  
  var dropped = 0L
  var submitted = 0L
  
  def get(buffer : ArrayBuffer[LatencyTask[T,D]]) = {
    if(startTime == 0L)
      startTime = System.nanoTime()
    
    val now = System.nanoTime()
    val time = (now - startTime) / 1000000000.0
    
    val shouldHaveSubmitted = (time * throughput).toLong
    val deficit = shouldHaveSubmitted - submitted
  
    submitted = shouldHaveSubmitted
    
    var gen = deficit
    if(deficit > capacity) {
      dropped += deficit - capacity
      gen = capacity
    }
      
    val interval = 1.0 / throughput
    
    var i = gen
    while(i > 0) {
      i -= 1
      val task = new LatencyTask[T,D](generate(), reporter)
      task.start -= ((i * interval) * 1000000000).toLong      // Compute correct starting time
      buffer += task
    }
  }
  
  def hasTasks : Boolean = {
    val now = System.nanoTime()
    val time = (now - startTime) / 1000000000.0
    
    val shouldHaveSubmitted = (time * throughput).toLong
    val deficit = shouldHaveSubmitted - submitted
    
    deficit > 0
  }
}

case class WorkerReport(waiting : Double, generating : Double, queueing : Double, committing : Double, notifying : Double, forcing : Double) {
  def totalTime = waiting + generating + queueing + committing + notifying + forcing
  def merge(other : WorkerReport) = new WorkerReport(waiting + other.waiting, generating + other.generating, queueing + other.queueing, committing + other.committing, notifying + other.notifying, forcing + other.forcing)
}


class Worker[T >: Null, D <: Diff[T]](tq : Generator[T,D], newDiff : () => D, state : State[T], started : AtomicBoolean, stopped : AtomicBoolean) extends Thread {  
  var report : WorkerReport = null
  
  override def run() {
    var tasks = new ArrayBuffer[LatencyTask[T,D]]
    
    val timer = new Timer()
    
    var totalWait = 0.0
    var totalGen = 0.0
    var totalEnqueue = 0.0
    var totalCommit = 0.0
    var totalNotify = 0.0
    var totalForce = 0.0
    
    var concurrencyTime = 0.0
    var totalConcurrency = 0.0
    
    val gcs = ManagementFactory.getGarbageCollectorMXBeans()
    val gcCount = () => {
      var count = 0L
      var i = gcs.length
      while(i > 0) {
        i -= 1
        count += gcs(i).getCollectionCount
      }
      count
    }
    var lastGcCount = gcCount()
    
    // while(!started.get) {}
    while(!stopped.get) {
      val diff = newDiff()
      
      timer.lap
      
      // Wait for tasks
      while(!tq.hasTasks && !stopped.get) {}
      
      val start = timer.lap
      
      if(!stopped.get) {
        // Get new tasks      
        tq.get(tasks)
        
        val concurrency = tasks.length
        
        val genTime = timer.lap
        
        // Enqueue tasks
        for(task <- tasks)
          task.enqueue(diff)
          
        val enqueueTime = timer.lap
          
        // Commit
        val ns = state.update(diff.apply(_))
        
        val commitTime = timer.lap
        
        // Force
        diff.force(ns)
        
        val forceTime = timer.lap
        
        // Notify tasks
        for(task <- tasks)
          task.result(ns)
                
        tasks = new ArrayBuffer[LatencyTask[T,D]]
        
        val notifyTime = timer.lap
        
        val gcc = gcCount()
        if(lastGcCount == gcc) {
          concurrencyTime += start
          totalWait += start
        
          totalGen += genTime
          totalEnqueue += enqueueTime
          totalCommit += commitTime
          totalForce += forceTime
          totalNotify += notifyTime
        } else {
          lastGcCount = gcc
        }
      }
    }

    report = new WorkerReport(totalWait, totalGen, totalEnqueue, totalCommit, totalNotify, totalForce)
  }
}

class Timer {
  var last = System.nanoTime()
  
  def lap = {
    val now = System.nanoTime()
    val elapsed = (now - last) / 1000000000.0
    last = now
    elapsed
  }
}