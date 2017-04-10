package lazyvar2

import java.util.concurrent.ThreadLocalRandom

object Main {
  val txcount = 100000
  
  def main(args : Array[String]) : Unit = {
    val state = Array.ofDim[LazyVar[Int]](100)
    for(i <- 0 until state.length)
      state(i) = new LazyVar(0)
    
    // note: lazy reads do not improve performance in this model, because they must be locked anyway, so they could be read immediately
    // they do improve performance if they can actually be read lazily
    
    while(true) {
      val begin = System.nanoTime()
      for(i <- (0 to txcount).par) {
        val rnd = ThreadLocalRandom.current()
        
        val tx = new Transaction()
        for(i <- 1 to 10)
          tx.update(state(rnd.nextInt(state.size)), (x : Int) => x + 1)
        tx.commit
      }
      val end = System.nanoTime()
      
      val time = (end - begin) / 1000000000.0
      
      println(txcount / time)
    }
  }
}