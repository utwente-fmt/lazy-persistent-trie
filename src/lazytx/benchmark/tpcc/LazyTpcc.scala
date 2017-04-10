package lazytx.benchmark.tpcc

import java.util.HashSet
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.Buffer
import lazytrie.ComposableMap.toComposableMap
import lazytrie.Map
import scala.collection.mutable.{Set => MSet, Map => MMap}
import lazytrie.SmallIntKey
import lazytrie.LazyArray
import lazytrie.Set
import lazytx.State
import laziness.Lazy

case class Warehouse(name : String, tax : Double, ytd : Double, districts : LazyArray[District], stock : Map[Int, Stock]) extends Lazy[Warehouse] {
  def get = this
  def evaluate = this
  def isEvaluated = true
}
case class District(next_o_id : Int, tax : Double, ytd : Double, customers : Map[Int, Customer], customer_name_index : MMap[String, MSet[Int]], orders : Map[Int, Order], new_orders : Set[Int], last_delivery_id : Int) extends Lazy[District] {
  def get = this
  def evaluate = this
  def isEvaluated = true
}
case class Customer(first : String, last : String, credit : String, data : String, discount : Double, balance : Double, ytd_payment : Double, payment_cnt : Int, delivery_cnt : Int, last_order_id : Int)
case class Order(c_id : Int, entry_d : Long, carrier_id : Option[Int], ol_cnt : Int, order_lines : LazyArray[OrderLine])
case class OrderLine(i_id : Int, supply_w_id : Int, delivery_d : Option[Long], quantity : Int, amount : Double) extends Lazy[OrderLine]  {
  def get = this
  def evaluate = this
  def isEvaluated = true
}
case class Item(name : String, price : Double, data : String)
case class Stock(quantity : Int, ytd : Int, order_cnt : Int, remote_cnt : Int, data : String)
case class Database(warehouses : Map[Int, Warehouse], items : MMap[Int, Item])

class LazyTpcc(val state : State[Database], warehouseCount : Int) extends Tpcc {
  val stockDiff = state.get.warehouses(1).stock.emptyMap[Option[Stock] => Option[Stock]]
  
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double = {
    // Optimistic phase
    val snapshot = state.get
    val warehouse = snapshot.warehouses(w_id)
    val district = warehouse.districts.get(d_id)
    val customer = district.customers(c_id)
    
    // Optimistically compute order lines and total amount of the order
    // We assume that all data accessed is immutable
    var sum_amount = 0.0
    var ol_id = 0
    val order_lines = LazyArray.ofDim[OrderLine](item_orders.length)
    item_orders.foreach(io => {
      val item = snapshot.items.get(io.i_id)
      if(item.isEmpty)
        return -1.0
      val amount = item.get.price * io.quantity
      order_lines(ol_id) = OrderLine(io.i_id, io.supply_w_id, None, io.quantity, amount)
      val bg = item.get.data.contains("ORIGINAL") && snapshot.warehouses(io.supply_w_id).stock(io.i_id).data.contains("ORIGINAL")
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
    val diff = snapshot.warehouses.emptyMap[Option[Warehouse] => Option[Warehouse]]
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
    val ns = state.update(db => {
      Database(db.warehouses.update(diff), db.items)
    })
    
    // Force evaluation
    val d = ns.warehouses(w_id).districts.get(d_id)
    if(o_id == -1) println("ERROR")
    d.orders.force(o_id)
    d.new_orders.force(o_id)
    d.customers.force(c_id)
    item_orders.foreach(io => ns.warehouses(io.supply_w_id).stock.force(io.i_id))
    
    total_amount
  }
  
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) : Unit = {
    val snapshot = state.get
    val c_id = get_customer_id(snapshot, c_w_id, c_d_id, c)
    if(c_id < 0)
      return
    
    val diff = snapshot.warehouses.emptyMap[Option[Warehouse] => Option[Warehouse]]
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
      
    val ns = state.update(db => {
      Database(db.warehouses.update(diff), db.items)
    })
    
    ns.warehouses(w_id).districts.get(d_id)
    ns.warehouses(c_w_id).districts.get(c_d_id).customers.force(c_id)
  }
  
  def order_status(w_id : Int, d_id : Int, c : Either[Int,String]) : Unit = {
    val snapshot = state.get
    val c_id = get_customer_id(snapshot, w_id, d_id, c)
    if(c_id < 0)
      return
      
    val w = snapshot.warehouses(w_id)
    val d = w.districts.get(d_id)
    val customer = d.customers(c_id)
    
    if(customer.last_order_id != -1) {
      d.orders.get(customer.last_order_id)
    }
  }
  
  val empty_o_ids = Array.ofDim[Int](10).map(_ => -1)
  def delivery(w_id : Int, o_carrier_id : Int) : Boolean = {
    var skipped = false
    var o_ids = empty_o_ids.clone()
    
    val ns = state.update(db => {
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
    ns.warehouses(w_id).districts.foreach((d_id, d) => {
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

    return skipped
  }
  
  def stock_level(w_id : Int, d_id : Int, threshold : Int) : Int = {
    val snapshot = state.get
    
    val w = snapshot.warehouses(w_id)
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
  
  private def get_customer_id(snapshot : Database, w_id : Int, d_id : Int, c : Either[Int,String]) : Int =
    if(c.isRight) {
      val warehouse = snapshot.warehouses(w_id)
      val district = warehouse.districts.get(d_id)
      district.customer_name_index.get(c.right.get) match {
        case Some(c_ids) => {
          var buffer = Buffer[(Int,Customer)]()
          for(k <- c_ids) {
            buffer += ((k, district.customers(k)))
          }
          buffer.sortBy(_._2.first)
          buffer((buffer.length / 2).toInt)._1
        }
        case None => -1
      }
    } else c.left.get
}

object LazyTpcc {
  val maxOrders = 3000  // max orders per district, minimum is 3000
  
  def generate(warehouse_count : Int) : Database = {
    val emptyStock = Map[Int, Stock]()(SmallIntKey.make(100000))
    val emptyDistricts = Map[Int, District](4)(SmallIntKey.make(10))

    var items = MMap[Int,Item]()
    for(i_id <- 1 to 100000) {
      items(i_id) = Item(Util.randa(Util.rand(14, 24)), Util.rand(100, 10000) * 0.01, Util.randOriginal)
    }
    
    var warehouses = Map[Int, Warehouse]()(SmallIntKey.make(warehouse_count))
    for(w_id <- 1 to warehouse_count) {
      var stock = emptyStock.empty
      for(i_id <- 1 to 100000) {
        stock(i_id) = Stock(Util.rand(10, 100), 0, 0, 0, Util.randOriginal)
      }
      
      var districts = LazyArray.ofDim[District](11)
      for(d_id <- 1 to 10) {
        districts(d_id) = District(3001, Util.rand(0, 2000) * 0.0001, 30000.0,
          Map[Int, Customer]()(SmallIntKey.make(3000)),
          MMap[String,MSet[Int]](),
          Map[Int, Order]()(SmallIntKey.make(maxOrders)),
          lazytrie.Set[Int]()(SmallIntKey.make(maxOrders)),
          2100
        )
      }
      warehouses(w_id) = Warehouse(Util.randa(Util.rand(6, 10)), Util.rand(0, 2000) * 0.0001, 300000.0, districts, stock)
    }
    
    val c_index = MSet[Int]()
    warehouses.foreach((w_id, w) => {
      w.districts.foreach((d_id, d) => {
        for(c_id <- 1 to 3000) {
          val first = Util.name(Util.rand(0, 999))
          val last = Util.name(Util.rand(0, 999))
          d.customers(c_id) = Customer(first, last, if(Util.rand(0,10) == 0) "BC" else "GC", Util.randa(Util.rand(30, 50)), Util.rand(0, 5000) * 0.0001, -10.0, 10, 1, 0, -1)
          if(!d.customer_name_index.contains(last))
            d.customer_name_index(last) = MSet()
          d.customer_name_index(last).add(c_id)
        }
        
        var o_id = 1
        for(c_id <- Util.permutation(3000)) {
          val ol_cnt = Util.rand(5, 15)
          var order_lines = LazyArray.ofDim[OrderLine](ol_cnt)
          for(number <- 1 to ol_cnt) {
            order_lines(number - 1) = OrderLine(Util.rand(1, 100000), w_id, Some(System.currentTimeMillis()), 5, 0.00)
          }
          d.orders(o_id) = Order(c_id, System.currentTimeMillis(), if(o_id < 2101) Some(Util.rand(1, 10)) else None, ol_cnt, order_lines)
          d.customers(c_id) = c => Some(c.get.copy(last_order_id = o_id))
          if(o_id >= 2101) {
            d.new_orders += o_id
          }
          o_id += 1
        }
      })
    })
    
    Database(warehouses, items)
  }
}
