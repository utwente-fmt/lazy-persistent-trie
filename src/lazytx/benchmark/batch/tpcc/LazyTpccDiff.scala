package lazytx.benchmark.batch.tpcc

import java.util.HashSet
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import lazytrie.Map
import lazytrie.Set
import lazytrie.SmallIntKey
import lazytrie.LazyArray
import lazytx.Diff
import lazytx.MapDiff
import lazytx.benchmark.tpcc.Customer
import lazytx.benchmark.tpcc.Database
import lazytx.benchmark.tpcc.District
import lazytx.benchmark.tpcc.ItemOrder
import lazytx.benchmark.tpcc.LazyTpcc
import lazytx.benchmark.tpcc.Order
import lazytx.benchmark.tpcc.OrderLine
import lazytx.benchmark.tpcc.Stock
import lazytx.benchmark.tpcc.Warehouse
import lazytx.DoubleDiff
import lazytx.IntDiff
import lazytx.State

case class WarehouseDiff(ytd : DoubleDiff, districts : Array[District => District], dforce : Array[List[District => Unit]], stock : MapDiff[Int, Stock]) {}

object DatabaseDiff {
  val emptyOrderLines = Map[Int, OrderLine](4)(new SmallIntKey(4))

  var emptyStockDiff : MapDiff[Int, Stock] = null
  
  def init(snapshot : Database) = {
    emptyStockDiff = snapshot.warehouses(1).stock.diff
  }
  
  def apply(snapshot : Database) = new DatabaseDiff(
    snapshot,
    snapshot.warehouses.emptyMap[WarehouseDiff]
  )
}

class DatabaseDiff(var optimistic : Database, warehouses : Map[Int, WarehouseDiff]) extends Diff[Database] {
  private def getWarehouse(w_id : Int) = {
     warehouses.get(w_id) match {
      case None =>
        val wd = new WarehouseDiff(new DoubleDiff(), Array.ofDim[District => District](11), Array.ofDim[List[District => Unit]](11), DatabaseDiff.emptyStockDiff.empty)
        warehouses(w_id) = wd
        wd
      case Some(wd) => wd
    }
  }
  
  def queueDistrict(wd : WarehouseDiff, d_id : Int, f : District => District, force : District => Unit = null) = {
    if(force != null) {
      if(wd.dforce(d_id) == null)
        wd.dforce(d_id) = List()
      wd.dforce(d_id) ::= force
    }
    if(wd.districts(d_id) == null)
      wd.districts(d_id) = f
    else
      wd.districts(d_id) = f compose wd.districts(d_id)
  }
  
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double = {
    // Compute order lines and total amount of the order
    var sum_amount = 0.0
    var ol_id = 1
    val order_lines = LazyArray.ofDim[OrderLine](item_orders.length + 1)
    item_orders.foreach(io => {
      val item = optimistic.items.get(io.i_id)
      if(item.isEmpty)
        return -1.0
      val amount = item.get.price * io.quantity
      order_lines(ol_id) = OrderLine(io.i_id, io.supply_w_id, None, io.quantity, amount)
      val bg = item.get.data.contains("ORIGINAL") && optimistic.warehouses(io.supply_w_id).stock(io.i_id).data.contains("ORIGINAL")
      sum_amount += amount
      ol_id += 1
    })
    
    val w = optimistic.warehouses(w_id)
    val d = w.districts.get(d_id)
    val c = d.customers(c_id)
    
    val total_amount = sum_amount * (1 - c.discount) * (1 + w.tax + d.tax) 
    
    item_orders.foreach(io => {
      getWarehouse(io.supply_w_id).stock.apply(io.i_id, s => {
        Stock(
          if(s.quantity >= io.quantity + 10) s.quantity - io.quantity else s.quantity - io.quantity + 91,
          s.ytd + io.quantity,
          s.order_cnt + 1,
          s.remote_cnt + (if(io.supply_w_id != w_id) 1 else 0),
          s.data
        )}
      )
    })
    
    val now = System.currentTimeMillis()
    val carrier_id = Some(ThreadLocalRandom.current().nextInt(10))
    
    var o_id = -1
    queueDistrict(getWarehouse(w_id), d_id, d => {
      o_id = d.next_o_id % LazyTpcc.maxOrders
      val next_orders = d.orders.put(o_id, () => Some(Order(c_id, now, carrier_id, item_orders.length, order_lines)))
      val next_new_orders = d.new_orders.add(o_id)
      val next_customers = d.customers.modify(c_id, _.copy(last_order_id = o_id))
      District((d.next_o_id + 1) % LazyTpcc.maxOrders, d.tax, d.ytd, next_customers, d.customer_name_index, next_orders, next_new_orders, d.last_delivery_id)
    }, d => {
      if(o_id == -1)
        println("ERROR")
      d.orders.force(o_id)
      d.new_orders.force(o_id)
      d.customers.force(c_id)
    })
    
    total_amount
  }
  
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) = {
    val c_id = get_customer_id(optimistic, c_w_id, c_d_id, c)
    
    if(c_id >= 0) {
      val wd = getWarehouse(w_id)
      wd.ytd.add(amount)
      
      queueDistrict(wd, d_id, d => d.copy(ytd = d.ytd + amount))
      queueDistrict(getWarehouse(c_w_id), c_d_id, d => {
        d.copy(customers = d.customers.modify(c_id, (c : Customer) => {
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
      }, d => {
        d.customers.force(c_id)
      })
    }
  }
  
  def delivery(w_id : Int, o_carrier_id : Int) : Database => Unit = {
    var skipped = false
    
    var o_ids = Array.ofDim[Int](10)
    for(i <- 0 to 9)
      o_ids(i) = -1
    
    val ol_delivery_d = Some(System.currentTimeMillis())
    val wd = getWarehouse(w_id)
    
    for(d_id <- 1 to 10) {      
      queueDistrict(wd, d_id, d => {
        var first = d.new_orders.next(d.last_delivery_id)
          if(!first.isDefined)
            first = d.new_orders.firstKey
            
        if(first.isDefined) {
          val o_id = first.get
          o_ids(d_id - 1) = o_id
          
          val next_new_orders = d.new_orders.remove(o_id)
          
          val next_orders = d.orders.modify(o_id, o => {
            val next_order_lines = o.order_lines.map((ol_id, ol) => ol.copy(delivery_d = ol_delivery_d))
            o.copy(carrier_id = Some(o_carrier_id), order_lines = next_order_lines)
          })
          
          val c_id = d.orders(o_id).c_id
          
          val next_customers = d.customers.modify(c_id, c => {
            c.copy(
              balance = c.balance + d.orders(o_id).order_lines.reduce(0.0, (acc : Double, ol) => acc + ol.amount), 
              delivery_cnt = c.delivery_cnt + 1
            )
          })
          
          d.copy(new_orders = next_new_orders, orders = next_orders, customers = next_customers, last_delivery_id = o_id)
        } else {
          skipped = true
          d
        }
      }, d => {
        val o_id = o_ids(d_id - 1)
        if(o_id != -1) {
          d.new_orders.force(o_id)
          val o = d.orders(o_id)
          o.order_lines.forceAll
          d.customers.force(o.c_id)
        }
      })
    }
    
    (_ : Database) => { skipped }
  }
  
  def order_status(w_id : Int, d_id : Int, c : Either[Int,String]) : Database => Unit = (state : Database) => {
    val c_id = get_customer_id(state, w_id, d_id, c)
    if(c_id >= 0) {
      val w = state.warehouses(w_id)
      val d = w.districts.get(d_id)
      val customer = d.customers(c_id)
      
      if(customer.last_order_id != -1) {
        d.orders.get(customer.last_order_id)
      }
    }
  }
  
  def stock_level(w_id : Int, d_id : Int, threshold : Int) : Database => Unit = (state : Database) => {
    val w = state.warehouses(w_id)
    val d = w.districts.get(d_id)
    
    val high = Math.max((d.next_o_id % LazyTpcc.maxOrders) - 1, 1).toInt
    val low = Math.max(high - 20, 1).toInt
   
    val seen = new HashSet[Int]()
    var count = 0
    
    for(i <- low to high) {
      d.orders(i).order_lines.foreach((ol_id, ol) => {
        if(!seen.contains(ol.i_id) && w.stock(ol.i_id).quantity < threshold) {
          count += 1
        }
        seen.add(ol.i_id)
      })
    }
    
    count
  }
  
  private def get_customer_id(snapshot : Database, w_id : Int, d_id : Int, c : Either[Int,String]) : Int =
    if(c.isRight) {
      val warehouse = snapshot.warehouses(w_id)
      val district = warehouse.districts.get(d_id)
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
  
  def apply(db : Database) : Database = {
    Database(  
      db.warehouses.mergeKD(warehouses, None, None, (k,w,wd : WarehouseDiff) => {
        var next_districts = w.districts.map((d_id,d) => {
            if(wd.districts(d_id) == null)
              d
            else
              wd.districts(d_id)(d)
          })
        
        Some(Warehouse(
          w.name,
          w.tax,
          wd.ytd.apply(w.ytd),
          next_districts,
          wd.stock.apply(w.stock)
        ))
      }), db.items)
  }
  
  def force(state : Database) = {
    optimistic = null
    
    warehouses.foreach((w_id, wd) => {
      // force warehouse
      val w = state.warehouses(w_id)
      wd.stock.force(w.stock)
      
      // force districts
      var i = 1
      while(i < 11) {
        if(wd.dforce(i) != null)
          wd.dforce(i).foreach(_(w.districts.get(i)))
        i += 1
      }
    })
  }
}


