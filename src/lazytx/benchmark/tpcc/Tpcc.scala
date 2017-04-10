package lazytx.benchmark.tpcc

import lazytx.State
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.lang.management.ManagementFactory
import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.Buffer

case class ItemOrder(i_id : Int, supply_w_id : Int, quantity : Int)

abstract class Tpcc {
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) : Unit
  def order_status(w_id : Int, d_id : Int, c : Either[Int,String]) : Unit
  def delivery(w_id : Int, o_carrier_id : Int) : Boolean
  def stock_level(w_id : Int, d_id : Int, threshold : Int) : Int
}

object Benchmark {  
  var DELIVERY = 4
  var ORDER_STATUS = 4
  var STOCK_LEVEL = 4
  var PAYMENT = 43
  
  def main(args : Array[String]) = {    
    val intro = 1.0
    val time = 3.0
    
    /*
    val t1gc = Buffer[Long]()
    val tngc = Buffer[Long]()
    val t1nogc = Buffer[Long]()
    val tnnogc = Buffer[Long]()
    
    for(warehouses <- Array(1,2,4,8,16,32,64,128)) {
      print(warehouses + " warehouses: ")
      
      var workload : Tpcc = null
      args(0) match {
        case "stm" =>
          val state = StmTpcc.generate(warehouses)
          workload = new StmTpcc(state, warehouses)
        case "lazy" =>
          val state = new State(LazyTpcc.generate(warehouses))
          workload = new LazyTpcc(state, warehouses)
        case "partition" =>
          workload = LazyPartitionTpcc(LazyTpcc.generate(warehouses))
        case "buffered" =>
          workload = LazyBufferedPartitionTpcc(LazyTpcc.generate(warehouses))
        case "locking" =>
          val state = LockingTpcc.generate(warehouses)
          workload = new LockingTpcc(state, warehouses)
      }
      
      val pinThreads = if(args.length > 1) args(1).toBoolean else false
      
      if(args.length > 2) {
        DELIVERY = args(2).toInt
        ORDER_STATUS = args(3).toInt
        STOCK_LEVEL = args(4).toInt
        PAYMENT = args(5).toInt
      }
      
      // Warmup
      //println("Warmup")
      //average(9, intro, time, 8, workload, warehouses, pinThreads)
      
      val t1 = average(9, intro, time, 1, workload, warehouses, pinThreads)
      val tn = average(9, intro, time, 64, workload, warehouses, pinThreads)
      
      println(t1._1.toLong, tn._1.toLong, (tn._1 / t1._1).toFloat)
      
      t1gc.add(t1._2.toLong)
      t1nogc.add(t1._1.toLong)
      
      tngc.add(tn._2.toLong)
      tnnogc.add(tn._1.toLong)
      
    }
    
    println("Without garbage collection:")
    println(" 1 thread:   " + t1nogc.mkString(" "))
    println(" 64 threads: " + tnnogc.mkString(" "))
    println(" speedup:    " + t1nogc.zip(tnnogc).map({ case (a,b) => b.toFloat/a}).mkString(" "))
    println("With garbage collection:")
    println(" 1 thread:   " + t1gc.mkString(" "))
    println(" 64 threads: " + tngc.mkString(" "))
    println(" speedup:    " + t1gc.zip(tngc).map({ case (a,b) => b.toFloat/a}).mkString(" "))
    */
    
    val warehouses = 1
    
    var workload : Tpcc = null
    args(0) match {
      case "stm" =>
        val state = StmTpcc.generate(warehouses)
        workload = new StmTpcc(state, warehouses)
      case "lazy" =>
        val state = new State(LazyTpcc.generate(warehouses))
        workload = new LazyTpcc(state, warehouses)
      case "partition" =>
        workload = LazyPartitionTpcc(LazyTpcc.generate(warehouses))
      case "buffered" =>
        workload = LazyBufferedPartitionTpcc(LazyTpcc.generate(warehouses))
      case "locking" =>
        val state = LockingTpcc.generate(warehouses)
        workload = new LockingTpcc(state, warehouses)
    }
    
    if(args.length > 1) {
      DELIVERY = args(1).toInt
      ORDER_STATUS = args(2).toInt
      STOCK_LEVEL = args(3).toInt
      PAYMENT = args(4).toInt
    }
    
    while(true) {
      val result = Buffer[Double]()
      
      var t1 = 0.0
      for(threads <- Array(1, 2, 4, 8, 16, 32, 64)) {
        val n = average(9, intro, time, threads, workload, warehouses, false)
        if(threads == 1)
          t1 = n._1
        println(n._1.toLong, n._1 / t1)
        
        result += n._1
      }
      println()
      println(result.map(_.toLong).mkString(" "))
      println(result.map(_ / result(0)).mkString(" "))
    }
  }
  
  def average(runs : Int, intro : Double, time : Double, threadCount : Int, tpcc : Tpcc, warehouseCount : Int, pinThreads : Boolean) : (Double, Double) = {
    var results = Buffer[Double]()
    var resultsgc = Buffer[Double]()
    for(i <- 1 to runs) {
      var r = benchmark(intro, time, threadCount, tpcc, warehouseCount, pinThreads)
      results += r._3
      resultsgc += r._1
      //System.err.println(r)
    }
    
    (
      results.sortBy(x => x).drop(2).dropRight(2).sum / (results.length - 4),
      resultsgc.sortBy(x => x).drop(2).dropRight(2).sum / (resultsgc.length - 4)
    )
  }
  
  def benchmark(intro : Double, time : Double, threadCount : Int, tpcc : Tpcc, warehouseCount : Int, pinThreads : Boolean) = {
    val running = new AtomicBoolean(true)
    var threads = List[BenchmarkThread]()
    
    if(pinThreads) {
      if(warehouseCount < threadCount) {
        val threadsPerWarehouse = threadCount / warehouseCount
        for(i <- 0 until threadCount) {
          val w_low = 1 + (i / threadsPerWarehouse) 
          threads ::= new BenchmarkThread(tpcc, warehouseCount, running, w_low, w_low)
        }
      } else {
        val warehousesPerThread = warehouseCount / threadCount
        for(i <- 0 until threadCount) {
          val w_low = 1 + i * warehousesPerThread
          threads ::= new BenchmarkThread(tpcc, warehouseCount, running, w_low, w_low + warehousesPerThread - 1)
        }
      }
    } else {
      for(i <- 0 until threadCount) {
        threads ::= new BenchmarkThread(tpcc, warehouseCount, running, 1, warehouseCount)
      }
    }
    
    threads.foreach(_.start())
    
    Thread.sleep((intro * 1000).toLong)
    
    var start = System.nanoTime()
    var startgc = getGarbageCollectionTime()
    
    threads.foreach(_.reset())
    Thread.sleep((time * 1000).toLong)
    running.set(false)
    
    val end = System.nanoTime()
    val endgc = getGarbageCollectionTime()
    val measuredTime = (end - start) / 1000000000.0
    val timegc = (endgc - startgc) / 1000.0
    val txs = threads.map(_.count.getAndSet(0)).sum / measuredTime
    val gcratio = timegc / measuredTime
    
    threads.foreach(_.join)
    
    (txs, gcratio, (txs / (1.0 - gcratio)))
  }
  
  def getGarbageCollectionTime() : Long = {
    var collectionTime = 0L;
    for (garbageCollectorMXBean <- ManagementFactory.getGarbageCollectorMXBeans().toList) {
        collectionTime += garbageCollectorMXBean.getCollectionTime();
    }
    collectionTime
  }
}

class BenchmarkThread(tpcc : Tpcc, warehouseCount : Int, running : AtomicBoolean, w_low : Int, w_high : Int) extends Thread {
  val count = new AtomicInteger(0)
  val a,b,c,d,e,f,g,h = 0L      // Padding, to make sure counts are not on same cache line

  def new_order() {
    val rnd = ThreadLocalRandom.current()
    
    val rbk = Util.rand(1, 100)
    val w_id = Util.rand(w_low, w_high)
    val ol_cnt = Util.rand(5, 15)
    var items = Array.ofDim[ItemOrder](ol_cnt)
    for(i <- 1 to ol_cnt) {
      val ol_i_id = if(i == ol_cnt && rbk == 1) Util.rand(100001, 1000000) else Util.nurand(8191, 1, 100000)
      val ol_w_id = if(rnd.nextDouble < 0.99)
        w_id
      else
        Util.rand(1, warehouseCount) // not correct: should select different warehouse if warehouses > 1
        
      items(i - 1) = ItemOrder(ol_i_id, ol_w_id, Util.rand(1, 10)) 
    } 
    
    val amount = tpcc.new_order(
      w_id,
      Util.rand(1, 10),
      Util.rand(1, 3000),
      items
    )
  }
  
  def payment() {
    val x = Util.rand(1, 100)
    val y = Util.rand(1, 100)
    
    val w_id = Util.rand(w_low, w_high)
    val d_id = Util.rand(1, 10)
    val c_w_id = if(x < 85) w_id else Util.rand(1, warehouseCount)     // TODO: exclude home warehouse
    val c_d_id = if(x < 85) d_id else Util.rand(1, 10)
    val c = if(y < 60) Right(Util.name(Util.nurand(255, 0, 999))) else Left(Util.nurand(1023, 1, 3000))
    
    tpcc.payment(w_id, d_id, c_w_id, c_d_id, c, Util.rand(100, 500000) * 0.01)
  }
  
  def order_status() {
    val x = Util.rand(1, 100)
    val w_id = Util.rand(w_low, w_high)
    val d_id = Util.rand(1, 10)
    val c = if(x < 60) Right(Util.name(Util.nurand(255, 0, 999))) else Left(Util.nurand(1023, 1, 3000))
    
    tpcc.order_status(w_id, d_id, c)
  }
  
  def delivery() {
    val w_id = Util.rand(w_low, w_high)
    val o_carrier_id = Util.rand(1, 10)
    
    tpcc.delivery(w_id, o_carrier_id)
  }
  
  def stock_level() {
    val w_id = Util.rand(w_low, w_high)
    val d_id = Util.rand(1, 10)
    val threshold = Util.rand(10, 20)
    
    tpcc.stock_level(w_id, d_id, threshold)
  }
  
  def mix() {
    val r = Util.rand(1, 100)
      
    if(r <= Benchmark.DELIVERY)
      delivery() 
    else if(r <= Benchmark.DELIVERY + Benchmark.ORDER_STATUS)
      order_status()
    else if(r <= Benchmark.DELIVERY + Benchmark.ORDER_STATUS + Benchmark.STOCK_LEVEL)
      stock_level()
    else if(r <= Benchmark.DELIVERY + Benchmark.ORDER_STATUS + Benchmark.STOCK_LEVEL + Benchmark.PAYMENT)
      payment()
    else
      new_order()
  }
  
  override def run() {
    while(running.get) {
      mix()
      if(running.get)
        count.incrementAndGet()
    }
  }
  
  def reset() = {
    count.set(0)
  }
}