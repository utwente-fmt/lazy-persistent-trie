package lazytx.benchmark.oltp

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.locks.ReentrantLock

import lazytrie.ComposableMap
import lazytrie.ComposableMap.toComposableMap
import lazytrie.Map
import lazytrie.test.Util

object TwoPhaseLocking {
    
    def main(args : Array[String]) : Unit = {
        for (i <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072)) {
          val partitionsTx = i
          val operationsTx = 10
          val partitionsElem = 100000
          val result = Benchmark.benchmark(8, 120000, workload(150, partitionsElem, partitionsTx, operationsTx))
          println("("+i+","+(result.toLong*partitionsTx*operationsTx).toDouble/1000000+")") 
        }
    }
    
    def workload(partitions: Int, partitionsElem: Int, partitionsTx: Int, operationsTx: Int) = {
      val db = Array.ofDim[Map[Int, Int]](partitions)
      val locks = Array.ofDim[ReentrantLock](partitions)
      
      for (i <- 0 to partitions-1) {
        db(i) = Util.createIntMap(partitionsElem)
        locks(i) = new ReentrantLock
      }
      
      () => {
        val random = ThreadLocalRandom.current()
        val partitionsNos = Array.ofDim[Int](partitionsTx)
        val diffs = Array.ofDim[Map[Int, Option[Int] => Option[Int]]](partitionsTx)
        val updatedTries = Array.ofDim[Map[Int, Int]](partitionsTx)
        
        for (i <- 0 to partitionsTx-1) {
          partitionsNos(i) = random.nextInt(partitions)
          
          val diff = db(0).emptyMap[Option[Int] => Option[Int]]
          
          for (j <- 0 to operationsTx-1) {
            diff.composeInPlace(random.nextInt(partitionsElem), _.map(_+1))
          }
          
          diffs(i) = diff
        }
        
        Arrays.sort(partitionsNos)
        
        partitionsNos.foreach(i => locks(i).lock)
        
        for ( i <- 0 to partitionsTx-1) {
          val trie = db(partitionsNos(i))
          val newtrie = trie.update(diffs(i))
          
          updatedTries(i) = newtrie
          
          db(partitionsNos(i)) = newtrie
        }
        
        partitionsNos.foreach(i => locks(i).unlock)
        
//       Kan benchmark beïnvloeden, kan updated keys apart opslaan in array 
        for (i <- 0 to partitionsTx-1) {
          for ((k,_) <- diffs(i)) {
            updatedTries(i).force(k)
          }
        }
        
      }
    }
}