package lazytx.benchmark.batch.tpcc

import lazytx.benchmark.batch.Workload
import lazytx.benchmark.tpcc.Database
import lazytx.benchmark.tpcc.LazyTpcc
import lazytx.State
import lazytx.benchmark.batch.Task
import lazytx.benchmark.tpcc.ItemOrder
import java.util.concurrent.ThreadLocalRandom
import lazytx.benchmark.tpcc.Util
import lazytx.benchmark.tpcc.Benchmark
import scala.collection.mutable.ArrayBuffer
import lazytx.MapDiff

class LazyTpccTask(warehouseCount : Int, w_id : Int) extends Task[Database, DatabaseDiffCoarse] {
  var resultf : Database => Unit = null
  
  def new_order(diff : DatabaseDiffCoarse) {
    val rnd = ThreadLocalRandom.current()
    
    val rbk = Util.rand(1, 100)
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
    
    val amount = diff.new_order(
      w_id,
      Util.rand(1, 10),
      Util.rand(1, 3000),
      items
    )
  }
  
  def payment(diff : DatabaseDiffCoarse) {
    val x = Util.rand(1, 100)
    val y = Util.rand(1, 100)
    
    val d_id = Util.rand(1, 10)
    val c_w_id = if(x < 85) w_id else Util.rand(1, warehouseCount)     // TODO: exclude home warehouse
    val c_d_id = if(x < 85) d_id else Util.rand(1, 10)
    val c = if(y < 60) Right(Util.name(Util.nurand(255, 0, 999))) else Left(Util.nurand(1023, 1, 3000))
    
    diff.payment(w_id, d_id, c_w_id, c_d_id, c, Util.rand(100, 500000) * 0.01)
  }
  
  def order_status(diff : DatabaseDiffCoarse) {
    val x = Util.rand(1, 100)
    val d_id = Util.rand(1, 10)
    val c = if(x < 60) Right(Util.name(Util.nurand(255, 0, 999))) else Left(Util.nurand(1023, 1, 3000))
    
    resultf = diff.order_status(w_id, d_id, c)
  }
  
  def delivery(diff : DatabaseDiffCoarse) {
    val o_carrier_id = Util.rand(1, 10)
    
    resultf = diff.delivery(w_id, o_carrier_id)
  }
  
  def stock_level(diff : DatabaseDiffCoarse) {
    val d_id = Util.rand(1, 10)
    val threshold = Util.rand(10, 20)
    
    resultf = diff.stock_level(w_id, d_id, threshold)
  }
  
  override def enqueue(diff : DatabaseDiffCoarse) = {
    val r = Util.rand(1, 100)
      
    if(r <= Benchmark.DELIVERY)
      delivery(diff) 
    else if(r <= Benchmark.DELIVERY + Benchmark.ORDER_STATUS)
      order_status(diff)
    else if(r <= Benchmark.DELIVERY + Benchmark.ORDER_STATUS + Benchmark.STOCK_LEVEL)
      stock_level(diff)
    else if(r <= Benchmark.DELIVERY + Benchmark.ORDER_STATUS + Benchmark.STOCK_LEVEL + Benchmark.PAYMENT)
      payment(diff)
    else
      new_order(diff)
  }
  
  override def result(state : Database) : Unit = {
    if(resultf != null)
      resultf(state)
  }
}

class LazyTpccWorkload(state : State[Database], warehouseCount : Int, w_low : Int, w_high : Int) extends Workload[Database, DatabaseDiffCoarse] {
  var localState = state.get
  var last = System.nanoTime()
  
  val mapDiff = MapDiff(state.get.warehouses)
  
  def makeTask = {
    val rnd = ThreadLocalRandom.current()
    new LazyTpccTask(warehouseCount, rnd.nextInt(w_high - w_low + 1) + w_low)
  }
  def emptyDiff = {
    var now = System.nanoTime()
    if(now - last > 1000000) {
      last = now
      localState = state.get
    }
    new DatabaseDiffCoarse(localState, (x : Database) => x, (x : Database) => x)
    // new DatabaseDiffCoarse(localState, mapDiff.empty, (x : Database) => x)
    // DatabaseDiff(localState)
  }
}