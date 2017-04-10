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
import lazytrie.ComposableMap.toComposableMap

class DatabaseDiffCoarse(var optimistic : Database, var update : Database => Database, var forcef : Database => Database) extends Diff[Database] { 
  val stockDiff = optimistic.warehouses(1).stock.emptyMap[Option[Stock] => Option[Stock]]
  val warehouseDiff = optimistic.warehouses.emptyMap[Option[Warehouse] => Option[Warehouse]]
  
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double = {
    // Optimistic phase
    val warehouse = optimistic.warehouses(w_id)
    val district = warehouse.districts.get(d_id)
    val customer = district.customers(c_id)
    
    // Optimistically compute order lines and total amount of the order
    // We assume that all data accessed is immutable
    var sum_amount = 0.0
    var ol_id = 0
    val order_lines = LazyArray.ofDim[OrderLine](item_orders.length)
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
    val total_amount = sum_amount * (1 - customer.discount) * (1 + warehouse.tax + district.tax)
    
    // Make a diff for stocks per warehouse
    val stockDiff = this.stockDiff.empty
    val stockDiffs = scala.collection.mutable.Map[Int,Map[Int,Option[Stock] => Option[Stock]]]()
    item_orders.foreach(io => {
      if(!stockDiffs.contains(io.supply_w_id))
        stockDiffs(io.supply_w_id) = stockDiff.empty
      
      stockDiffs(io.supply_w_id).modifyInPlace(io.i_id, s => Stock(
          if(s.quantity >= io.quantity + 10) s.quantity - io.quantity else s.quantity - io.quantity + 91,
          s.ytd + io.quantity,
          s.order_cnt + 1,
          s.remote_cnt + (if(io.supply_w_id != w_id) 1 else 0),
          s.data
        )
      )
    })
    
    var o_id = -1

    val now = System.currentTimeMillis()
    val carrier_id = Some(ThreadLocalRandom.current().nextInt(10))
    
    // Make a diff to insert the order into the warehouse, and update its stock
    val diff = warehouseDiff.empty
    diff.modifyInPlace(w_id, w => {
      val next_districts = w.districts.update(d_id, d => {
        o_id = d.next_o_id % LazyTpcc.maxOrders
        val next_orders = d.orders.put(o_id, () => Some(Order(c_id, now, carrier_id, item_orders.length, order_lines)))
        val next_new_orders = d.new_orders.add(o_id)
        val next_customers = d.customers.modify(c_id, _.copy(last_order_id = o_id))
        District((d.next_o_id + 1) % LazyTpcc.maxOrders, d.tax, d.ytd, next_customers, d.customer_name_index, next_orders, next_new_orders, d.last_delivery_id)
      })
      
      if(stockDiffs.contains(w_id))
        Warehouse(w.name, w.tax, w.ytd, next_districts, w.stock.update(stockDiffs(w_id)))
      else
        Warehouse(w.name, w.tax, w.ytd, next_districts, w.stock)
    })
    
    // Make a diff to update stock for other warehouses
    for((w,sd) <- stockDiffs) {
      if(w != w_id)
        diff.modifyInPlace(w, w => w.copy(stock = w.stock.update(sd)))
    }
    
    // Commit phase
    update = update compose (db => {
      Database(db.warehouses.update(diff), db.items)
    })
    
    // Force evaluation
    forcef = forcef compose (db => {
      val d = db.warehouses(w_id).districts.get(d_id)
      if(o_id == -1) println("ERROR")
      d.orders.force(o_id)
      d.new_orders.force(o_id)
      d.customers.force(c_id)
      item_orders.foreach(io => db.warehouses(io.supply_w_id).stock.force(io.i_id))
      db
    })
    
    total_amount
  }
  
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) : Unit = {
    val c_id = get_customer_id(optimistic, c_w_id, c_d_id, c)
    if(c_id < 0)
      return
    
    val diff = optimistic.warehouses.emptyMap[Option[Warehouse] => Option[Warehouse]]
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
      
    update = update compose (db => {
      Database(db.warehouses.update(diff), db.items)
    })
    
    forcef = forcef compose (db => {
      db.warehouses(w_id).districts.get(d_id)
      db.warehouses(c_w_id).districts.get(c_d_id).customers.force(c_id)
      db
    })
  }
  
  val empty_o_ids = Array.ofDim[Int](10).map(_ => -1)
  def delivery(w_id : Int, o_carrier_id : Int) : Database => Unit = {
    var skipped = false
    var o_ids = empty_o_ids.clone()
    
    update = update compose (db => {
      val ol_delivery_d = Some(System.currentTimeMillis())
      
      val next_warehouses = db.warehouses.modify(w_id, w => {
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
        w.copy(districts = next_districts)
      })
      
      Database(next_warehouses, db.items)
    })

    // force
    forcef = forcef compose (db => {
      db.warehouses(w_id).districts.foreach((d_id, d) => {
        val o_id = o_ids(d_id - 1)
        if(o_id != -1) {
          d.new_orders.force(o_id)
          val o = d.orders(o_id)
          o.order_lines.forceAll
          d.customers.force(o.c_id)
        }
        
        //if(ThreadLocalRandom.current().nextInt(20) == 0)
        //  d.new_orders.compact
      })
      db
    })
    
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
    update(db)
  }
  
  def force(state : Database) = {
    optimistic = null
    forcef(state)
  }
}


