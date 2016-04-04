package lazytx.benchmark.oltp

import java.util.concurrent.atomic.AtomicBoolean

object Benchmark {
  def benchmark(threads : Int, millis : Long, tx : () => Unit) : Double = {
    var start = time
    
    val running = new AtomicBoolean(false)
    val stopping = new AtomicBoolean(false)
    
    val ts = Array.ofDim[BenchmarkThread](threads)
    for(i <- 0 to threads - 1)
      ts(i) = new BenchmarkThread(running, stopping, tx)
    
    ts.foreach(_.start())
    
    val begin = time()
    running.set(true)
    Thread.sleep(millis)
    stopping.set(true)
    val end = time()
    
    ts.foreach(_.join())
    
    return ts.map(_.count).sum / (end - begin)
  }
  
  def time() = System.nanoTime() / 1000000000.0
}

class BenchmarkThread(running : AtomicBoolean, stopping : AtomicBoolean, tx : () => Unit) extends Thread {
  var count = 0L
  
  override def run() {
    var count = 0L
    
    while(!running.get()) {
      // wait to start
    }
    
    while(!stopping.get()) {
      tx()
      if(!stopping.get())  // Dont count transactions that didnt finish on time
        count += 1
    }
    
    this.count = count  
  }
}