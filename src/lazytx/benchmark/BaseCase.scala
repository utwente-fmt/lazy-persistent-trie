package lazytx.benchmark

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom

import lazytrie.ComposableMap
import lazytrie.ComposableMap.toComposableMap
import lazytrie.Map
import lazytrie.test.Util
import lazytx.benchmark.oltp.Benchmark

object BaseCase {
    
    def main(args : Array[String]) : Unit = {
      for (j <- Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072)) {
//      Standard number of transactions per partition: 5
        val partitionsTx = 5
//      Standard number of operations per transaction: 10
        val operationsTx = 10
//      Standard number of variables per partition: 100000, normally there are 150 partitions, so multiply to keep same sized database
        val partitionsElem = j*150
//      Standard number of partitions: 150 HOWEVER this is the base case thus it has only ONE partition
//      Take into account that overall database should still have the same number of variables
        val result = Benchmark.benchmark(8, 120000, workload(1, partitionsElem, partitionsTx, operationsTx))
//        println(result.toLong*partitionsTx*operationsTx)
        println("("+j+","+(result.toLong*partitionsTx*operationsTx).toDouble/1000000+")")
//      println("-------------")
      }
    }
    
    def workload(partitions: Int, partitionsElem: Int, partitionsTx: Int, operationsTx: Int) = {
      val db = Array.ofDim[Map[Int, Int]](partitions)
      
      for (i <- 0 to partitions-1) {
        db(i) = Util.createIntMap(partitionsElem)
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
        
        for ( i <- 0 to partitionsTx-1) {
          val trie = db(partitionsNos(i))
          val newtrie = trie.update(diffs(i))
          
          updatedTries(i) = newtrie
          
          db(partitionsNos(i)) = newtrie
        }
        
        
//       Kan benchmark beïnvloeden, kan updated keys apart opslaan in array 
        for (i <- 0 to partitionsTx-1) {
          for ((k,_) <- diffs(i)) {
            updatedTries(i).force(k)
          }
        }
        
      }
    }
}