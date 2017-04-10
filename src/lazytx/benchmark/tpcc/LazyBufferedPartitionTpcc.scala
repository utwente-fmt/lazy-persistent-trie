package lazytx.benchmark.tpcc

import java.util.HashSet
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable.ArrayBuffer 

import laziness.Lazy
import laziness.Value
import lazytrie.ComposableDiff.toComposableDiff
import lazytrie.ComposableMap.toComposableMap
import lazytrie.Map
import lazytrie.Set
import lazytrie.SmallIntKey
import lazytrie.LazyArray
import lazytx.State
import scala.collection.mutable.{Set => MSet, Map => MMap}

class LazyBufferedPartitionTpcc(warehouseCount : Int, warehouses : Array[Warehouse], items : MMap[Int, Item]) extends Tpcc {
  val bufferSize = 64
  
  val forceBuffers = Array.ofDim[ArrayBuffer[Warehouse => Unit]](warehouseCount + 1)
  for(i <- 0 until forceBuffers.length)
    forceBuffers(i) = ArrayBuffer()
    
  val locks = Array.ofDim[Object](warehouseCount + 1).map(_ => new Object)
  val emptyDiff = Map[Int, Warehouse => Warehouse]()(SmallIntKey.make(warehouseCount + 1))
  val emptyOrderLines = Map[Int, OrderLine](4)(new SmallIntKey(4))

  val stale = Array.ofDim[Warehouse](warehouseCount + 1)
  for(i <- 1 to warehouseCount)
    stale(i) = warehouses(i * 16)
  
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double = {
    // Optimistic phase
    val warehouse = stale(w_id)
    val district = warehouse.districts.get(d_id)
    val customer = district.customers(c_id)
    
    // Optimistically compute order lines and total amount of the order
    // We assume that all data accessed is immutable
    var sum_amount = 0.0
    var ol_id = 1
    val order_lines = LazyArray.ofDim[OrderLine](item_orders.length)
    item_orders.foreach(io => {
      val item = items.get(io.i_id)
      if(item.isEmpty)
        return -1.0
      val amount = item.get.price * io.quantity
      order_lines(ol_id - 1) = OrderLine(io.i_id, io.supply_w_id, None, io.quantity, amount)
      val bg = item.get.data.contains("ORIGINAL") && (if(io.supply_w_id == w_id) warehouse else stale(io.supply_w_id)).stock(io.i_id).data.contains("ORIGINAL")
      sum_amount += amount
      ol_id += 1
    })
    val total_amount = sum_amount * (1 - customer.discount) * (1 + warehouse.tax + district.tax)
    
    var now = System.currentTimeMillis()

    // Make a diff for stocks per warehouse
    val stock = warehouse.stock
    val stockDiffs = scala.collection.mutable.Map[Int,Map[Int,Option[Stock] => Option[Stock]]]()
    item_orders.foreach(io => {
      if(!stockDiffs.contains(io.supply_w_id))
        stockDiffs(io.supply_w_id) = stock.emptyMap[Option[Stock] => Option[Stock]]
      
      stockDiffs(io.supply_w_id).modifyInPlace(io.i_id, s => Stock(
          if(s.quantity >= io.quantity + 10) s.quantity - io.quantity else s.quantity - io.quantity + 91,
          s.ytd + io.quantity,
          s.order_cnt + 1,
          s.remote_cnt + (if(io.supply_w_id != w_id) 1 else 0),
          s.data
        )
      )
    })
    
    @volatile var o_id = 0

    val random = ThreadLocalRandom.current()
    val carrier_id = Some(random.nextInt(10))
    
    // Make a diff to insert the order into the warehouse, and update its stock
    val diff = emptyDiff.empty
    val add = stockDiffs.contains(w_id)
    diff.modifyInPlace(w_id, w => {
      val next_districts = w.districts.update(d_id, d => {
        o_id = d.next_o_id % LazyTpcc.maxOrders
        val next_orders = d.orders.put(o_id, () => Some(Order(c_id, now, carrier_id, item_orders.length, order_lines)))
        val next_new_orders = d.new_orders.add(o_id)
        val next_customers = d.customers.modify(c_id, _.copy(last_order_id = o_id))
        District((d.next_o_id + 1) % LazyTpcc.maxOrders, d.tax, d.ytd, next_customers, d.customer_name_index, next_orders, next_new_orders, d.last_delivery_id)
      })
      
      Warehouse(w.name, w.tax, w.ytd, next_districts, if(add) w.stock.update(stockDiffs(w_id)) else w.stock)
    })
    
    for((w,sd) <- stockDiffs) {
      if(w != w_id)
        diff.modifyInPlace(w, w => w.copy(stock = w.stock.update(sd)))
    }
 
    // Force evaluation
    val force = scala.collection.mutable.Map[Int, Warehouse => Unit]()
    force(w_id) = (w => {
      w.stock.force(stockDiffs(w_id))
      val d = w.districts.get(d_id)
      d.orders.force(o_id)
      d.new_orders.force(o_id)
      d.customers.force(c_id)
    })
    
    for((r_w_id,diff) <- stockDiffs) {
      if(r_w_id != w_id)
        force(r_w_id) = (w => w.stock.force(diff))
    }
    
    // Commit
    commit(diff.iterator, force)
    for((w_id,f) <- force) {
      f(warehouses(w_id * 16))
    }
    
    if(random.nextInt(10000) == 0) {
      for(i <- 1 to warehouseCount)
        stale(i) = warehouses(i * 16)
    }
    
    total_amount
  }
  
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) : Unit = {
    val c_id = get_customer_id(stale(c_w_id), c_d_id, c)
    if(c_id < 0)
      return
    
    val diff = emptyDiff.empty
    diff.modifyInPlace(w_id, w => w.copy(
      ytd = w.ytd + amount,
      districts = w.districts.update(d_id, d => d.copy(ytd = d.ytd + amount))
    ))
    diff.modifyInPlace(c_w_id, w => {
      w.copy(districts = w.districts.update(c_d_id, d => {
        d.copy(customers = d.customers.modify(c_id, c => {
          var next_data = c.data
          if(c.credit == "BC") {
            next_data = (c_id + "," + c_d_id + "," + c_w_id + "," + d_id + "," + w_id + "," + amount + "/" + c.data)
            if(next_data.length() > 50)
              next_data = next_data.substring(0, 50)
          }
          c.copy(
            balance = c.balance + amount,
            ytd_payment = c.ytd_payment + amount, 
            payment_cnt = c.payment_cnt + 1, 
            data = next_data
          )
        }))
      }))
    })
    
    val force = scala.collection.mutable.Map[Int, Warehouse => Unit]()
    force(c_w_id) = (w => w.districts.get(c_d_id).customers.force(c_id))
    
    commit(diff.iterator, force)
    for((w_id,f) <- force) {
      f(warehouses(w_id * 16))
    }
  }
  
  def order_status(w_id : Int, d_id : Int, c : Either[Int,String]) : Unit = {
    val w = warehouses(w_id * 16)
    val c_id = get_customer_id(w, d_id, c)
    if(c_id < 0) {
      return
    }  
    val d = w.districts.get(d_id)
    val customer = d.customers(c_id)
    
    if(customer.last_order_id != -1) {
      d.orders.get(customer.last_order_id)
    }
  }
  
  val empty_o_ids = Array.ofDim[Int](10).map(_ => -1)
  def delivery(w_id : Int, o_carrier_id : Int) : Boolean = {    
    var skipped = false
    var o_ids = empty_o_ids.clone

    val force = (w : Warehouse) => {
      w.districts.foreach((d_id,d) => {
        val o_id = o_ids(d_id - 1)
        if(o_id != -1) {
          val o = d.orders(o_id)
          o.order_lines.forceAll
          d.customers.force(o.c_id)
        }
      })
    }
      
    val ol_delivery_d = Some(System.currentTimeMillis())
    locks(w_id).synchronized {
      val w = warehouses(w_id * 16)
      
      val next_districts = w.districts.map((d_id, d) => {
        var first = d.new_orders.next(d.last_delivery_id)
        if(!first.isDefined)
          first = d.new_orders.firstKey
        if(first.isDefined) {
          val o_id = first.get
          o_ids(d_id - 1) = o_id
          
          val next_new_orders = d.new_orders.remove(o_id)
          
          val next_orders = d.orders.modify(o_id, o => {
            val next_order_lines = o.order_lines.map((_, ol) => ol.copy(delivery_d = ol_delivery_d))
            o.copy(carrier_id = Some(o_carrier_id), order_lines = next_order_lines)
          })
          
          val c_id = d.orders(o_id).c_id
          
          val next_customers = d.customers.modify(c_id, c => {
            c.copy(
              balance = c.balance + d.orders(o_id).order_lines.reduce(0.0, (acc : Double, ol) => acc + ol.amount), 
              delivery_cnt = c.delivery_cnt + 1
            )
          })
          
          d.copy(new_orders = next_new_orders, orders = next_orders, customers = next_customers, last_delivery_id=o_id)
        } else {
          skipped = true
          d
        }
      })
      
      forceBuffers(w_id) += force
      
      warehouses(w_id * 16) = w.copy(districts = next_districts)
    }
    
    return skipped
  }
  
  def stock_level(w_id : Int, d_id : Int, threshold : Int) : Int = {
    val w = warehouses(w_id * 16)
    val d = w.districts.get(d_id)
    
    val high = Math.max(d.next_o_id - 1, 1).toInt
    val low = Math.max(high - 20, 1).toInt
   
    val seen = new HashSet[Int]()
    var count = 0
    
    for(i <- low to high) {
      val o = d.orders(i)
      o.order_lines.foreach((_, ol) => {
        if(!seen.contains(ol.i_id) && w.stock(ol.i_id).quantity < threshold) {
          count += 1
        }
        seen.add(ol.i_id)
      })
    }
    
    count
  }
  
  private def get_customer_id(w : Warehouse, d_id : Int, c : Either[Int,String]) : Int =
    if(c.isRight) {
      val district = w.districts.get(d_id)
      district.customer_name_index.get(c.right.get) match {
        case Some(c_ids) => {
          var buffer = ArrayBuffer[(Int,Customer)]()
          for(k <- c_ids) {
            buffer += ((k, district.customers(k)))
          }
          buffer.sortBy(_._2.first)
          buffer((buffer.length / 2).toInt)._1
        }
        case None => -1
      }
    } else c.left.get
    
  private def commit(it : Iterator[(Int, Warehouse => Warehouse)], force : scala.collection.mutable.Map[Int, Warehouse => Unit]) : Unit = {
    val (w_id, f) = it.next
    locks(w_id).synchronized {
      warehouses(w_id * 16) = f(warehouses(w_id * 16))
      
      if(force.contains(w_id)) {
        forceBuffers(w_id) += force(w_id)
        if(forceBuffers(w_id).length > bufferSize) {
          val fb = forceBuffers(w_id).clone()
          forceBuffers(w_id).clear()
          force(w_id) = (w => fb.foreach(_(w)))
        } else {
          force.remove(w_id)
        }
      }
      
      if(it.hasNext) {
        commit(it, force)
      }
    }
  }
}

object LazyBufferedPartitionTpcc {
  val maxItems = 100000   // normally 100000
  val maxOrders = 3000   // max orders per district, minimum is 3000
  
  def apply(db : Database) = {
    val warehouseCount = db.warehouses.size
    var warehouses = Array.ofDim[Warehouse](warehouseCount * 16 + 1)
    for(i <- 1 to warehouseCount)
      warehouses(i * 16) = db.warehouses(i)
    
    new LazyBufferedPartitionTpcc(warehouseCount, warehouses, db.items)
  }
}