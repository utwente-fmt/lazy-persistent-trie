package lazytx.benchmark.transform

import java.io.OutputStream
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

abstract class TransformationBenchmarkReporter {
  def tx()
  def intro()
  def beginTransform()
  def endTransform()
  def end()
  def report(out : OutputStream)
}

class GraphReporter(bucketSize : Double) extends TransformationBenchmarkReporter {
  @volatile var running = false
  
  @volatile var introStart = -1L
  var transformStart = 0L
  var transformEnd = 0L
  var outroEnd = 0L
  
  val buckets = new ArrayBuffer[Int]()
  val currentCount = new AtomicInteger(0)
  
  def tx() = {
    if(running) {
      val time = System.currentTimeMillis()
      val bucket = (((time - introStart) / 1000.0) / bucketSize).toInt

      if(bucket > 1000)
        println(bucket, time, introStart, transformStart, transformEnd, outroEnd)
      
      this.synchronized {
        if(bucket > buckets.size) {
          buckets += currentCount.getAndSet(0)
          while(bucket > buckets.size) {
            buckets += 0
          }
        }
      }
      
      currentCount.incrementAndGet()
    }
  }
    
  def intro() = {
    introStart = System.currentTimeMillis()
    running = true
  }
    
  def beginTransform() = {
    transformStart = System.currentTimeMillis()
  }
  
  def endTransform() = {
    transformEnd = System.currentTimeMillis()
  }
    
  def end() = {
    running = false
    outroEnd = System.currentTimeMillis()
  }
  
  def report(out : OutputStream) = {
    // Pad with empty buckets if necessary
    /*
    val bucket = (((outroEnd - introStart) / 1000000000.0) / bucketSize).toInt
    if(bucket > buckets.size) {
      buckets += currentCount.getAndSet(0)
      while(bucket > buckets.size) {
        buckets += 0
      }
    }
    */

    var i = 0
    
    val printer = new PrintStream(out)
    printer.println(bucketSize)
    printer.println(format(transformStart - introStart))
    printer.println(format(transformEnd - introStart))
    buckets.map(_ / bucketSize).foreach(printer.println)
    printer.flush()
  }
  
  def format(time : Long) : String = (time / 1000.0).toString
}