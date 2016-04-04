package lazytx.benchmark.transform

import java.io.FileOutputStream
import java.util.concurrent.ThreadLocalRandom
import lazytrie.Map
import lazytx.State
import lazytrie.test.Util

object Main {
  def main(args : Array[String]) = {
    val dbsize = 10000000
    val bucketSize = 1.0
    val warmupTime = 10.0 //60.0
    val introTime = 10.0 //120.0
    val totalTime = 10.0 //600.0
    val concurrency = 64
    
    println("Building state")
    
    val state = new State(Util.createIntMap(dbsize))
    
    var r = 0
    while(true) {
      var i = 0
      for(workload <- Array("read", "update")) {
        for(reads <- Array(0, 1, 2, 4, 8, 16)) {
          println(reads + " reads")
          
          val transform = transformWorkload(state, dbsize, reads)
          
          val wl = workload match {
            case "read" => readWorkload(state, dbsize, 100)
            case "update" => updateWorkload(state, dbsize, 100)
          }
          
          val reporter = new GraphReporter(bucketSize)
          TransformationBenchmark.benchmark(reporter, concurrency, transform, wl, warmupTime, introTime, totalTime)
          reporter.report(System.out)
          val fos = new FileOutputStream("run" + r + "-" + workload + "-" + i + "reads")
          reporter.report(fos)
          fos.close()
          i += 1
        }
      }
      r += 1
    }
  }
  
  def transformWorkload(state : State[Map[Int,Int]], dbsize : Int, reads : Int) = () => {
    val ns = state.update(s => {
      s.updateAll(v => {
        val rnd = ThreadLocalRandom.current()
        var r = rnd.nextInt(1000)
        var i = 0
        while(i < reads) {
          r += s.get(rnd.nextInt(dbsize)).get
          i += 1
        }
        Some(v + r)
      })
    })
    ns.forceRoot
    ns.forceAll
  }
  
  def updateWorkload(state : State[Map[Int,Int]], dbsize : Int, txsize : Int) = () => {
    var diff = state.get.empty
    val keys = Array.ofDim[Int](txsize)
    val rnd = ThreadLocalRandom.current()
    
    var i = 0
    while(i < txsize) {
      val k = rnd.nextInt(dbsize)
      val v = rnd.nextInt(1000000)
      diff.putInPlace(k, v)
      keys(i) = k
      i += 1
    }
    val ns = state.update(m => { m.write(diff) })
    
    var j = 0
    ns.forceRoot
    while(j < txsize) {
      ns.force(keys(j))
      j += 1
    }
  }
  
  def readWorkload(state : State[Map[Int,Int]], dbsize : Int, txsize : Int) = () => {
    val rnd = ThreadLocalRandom.current()
    val s = state.get
    var sum = 0
    var i = 0
    while(i < txsize) {
      sum += s.get(rnd.nextInt(dbsize)).get
      i += 1
    }
  }
}