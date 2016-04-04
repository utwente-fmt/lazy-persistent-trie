package lazytx.benchmark.transform

import scala.collection.mutable.ArrayBuffer

object TransformationBenchmark {
  def benchmark(reporter : TransformationBenchmarkReporter, threadCount : Int, transform : () => Unit, workload : () => Unit, warmupTime : Double, introTime : Double, totalTime : Double) = {
    var done = false;
    var threads = ArrayBuffer[Thread]()
    for(i <- 1 to threadCount) {
      threads += new Thread() {
        override def run() {
          while(!done) {
            workload()
            reporter.tx()
          }
        }
      }
    }
    
    threads.foreach(_.start())
    
    Thread.sleep((warmupTime * 1000).toLong)
    
    System.gc()
    val start = System.nanoTime()
    
    reporter.intro()
    Thread.sleep((introTime * 1000).toLong)
      
    reporter.beginTransform()
    transform()
    reporter.endTransform()
    val now = System.nanoTime()
    val passed = (now - start) / 1000000000.0
    val remaining = totalTime - passed
    if(remaining > 0)
      Thread.sleep((remaining * 1000).toLong)
    reporter.end()
    
    done = true;
    threads.foreach(_.join())
  }
}