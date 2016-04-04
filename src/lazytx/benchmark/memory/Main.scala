package lazytx.benchmark.memory

import lazytrie.IntKey
import lazytrie.Key
import lazytrie.SmallIntKey
import java.util.concurrent.ThreadLocalRandom
import lazytrie.test.Util

object Main {
  val leafWidth = 0
  val branchWidth = 5
  
  def main(args : Array[String]) = {
    var s = 0

    // Warmup for timings
    for(i <- 0 to 100000)
      Util.createIntMap(1000, leafWidth, branchWidth)
    
    val runtime = Runtime.getRuntime
    for(size <- Array(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)) {
      System.gc()
      val before = runtime.totalMemory() - runtime.freeMemory()
      val begin = System.nanoTime()
      val trie = Util.createIntMap(size, leafWidth, branchWidth)
      val end = System.nanoTime()
      System.gc()
      val after = runtime.totalMemory() - runtime.freeMemory()
      
      // Ensures that the trie is not garbage collected immediately after creating it
      s += trie.get(0).getOrElse(0)
      
      println(size + ": " + (after - before) + " bytes, " + ((end - begin) / 1000000.0) + " ms, " + ((after - before) / size.toDouble) + " bytes per entry")
    }
    
    println(s)
  }
}