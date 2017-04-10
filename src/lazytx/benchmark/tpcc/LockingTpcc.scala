package lazytx.benchmark.tpcc

import java.util.HashSet
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.Buffer
import scala.collection.mutable.Map
import scala.collection.mutable.Set

case class LockingWarehouse(name : String, tax : Double, var ytd : Double, districts : Array[LockingDistrict], stock : Map[Int, LockingStock])
case class LockingDistrict(var next_o_id : Int, tax : Double, var ytd : Double, customers : Map[Int, LockingCustomer], customer_name_index : Map[String, Set[Int]], orders : Map[Int, LockingOrder], new_orders : Set[Int])
case class LockingCustomer(first : String, last : String, credit : String, var data : String, discount : Double, var balance : Double, var ytd_payment : Double, var payment_cnt : Int, var delivery_cnt : Int, var last_order_id : Int)
case class LockingOrder(c_id : Int, entry_d : Long, carrier_id : Option[Int], ol_cnt : Int, order_lines : Array[LockingOrderLine])
case class LockingOrderLine(i_id : Int, supply_w_id : Int, var delivery_d : Option[Long], quantity : Int, amount : Double)
case class LockingItem(name : String, price : Double, data : String)
case class LockingStock(var quantity : Int, var ytd : Int, var order_cnt : Int, var remote_cnt : Int, data : String)
case class LockingDatabase(warehouses : Array[LockingWarehouse], items : Map[Int, LockingItem])

class LockingTpcc(val state : LockingDatabase, warehouseCount : Int) extends Tpcc {
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double = {
    val warehouse = state.warehouses(w_id)
    val district = warehouse.districts(d_id)
    val customer = district.customers(c_id)
    
    var sum_amount = 0.0
    var ol_id = 0
    val order_lines = Array.ofDim[LockingOrderLine](item_orders.length)
    item_orders.foreach(io => {
      val item = state.items.get(io.i_id)
      if(item.isEmpty)
        return -1
      val amount = item.get.price * io.quantity
      order_lines(ol_id) = LockingOrderLine(io.i_id, io.supply_w_id, None, io.quantity, amount)
      val bg = item.get.data.contains("ORIGINAL") && state.warehouses(io.supply_w_id).stock(io.i_id).data.contains("ORIGINAL")
      sum_amount += amount
      ol_id += 1
    })
    val total_amount = sum_amount * (1 - customer.discount) * (1 + warehouse.tax + district.tax)
    
    val carrier_id = Some(ThreadLocalRandom.current().nextInt(10))
    val now = System.currentTimeMillis()
    
    update_stock(item_orders.sortBy(io => (io.supply_w_id, io.i_id)).iterator, w_id, () => {
      // Add order
      district.synchronized {
        val o_id = district.next_o_id % LockingTpcc.maxOrders
      
        district.orders(o_id) = LockingOrder(c_id, now, carrier_id, item_orders.length, order_lines) 
        district.new_orders.add(o_id)
        district.customers(c_id).last_order_id = o_id
        district.next_o_id = (district.next_o_id + 1) % LockingTpcc.maxOrders 
      }
    })
    
    0.0
  }
  
  def update_stock(it : Iterator[ItemOrder], w_id : Int, cont : () => Unit) : Unit = {
    if(it.hasNext) {
      val io = it.next()
      val s = state.warehouses(io.supply_w_id).stock(io.i_id)
      s.synchronized {
        if(s.quantity >= io.quantity + 10)
          s.quantity -= io.quantity
        else
          s.quantity -= io.quantity + 91
        s.ytd += io.quantity
        s.order_cnt += 1
        s.remote_cnt += (if(io.supply_w_id != w_id) 1 else 0)
        
        update_stock(it, w_id, cont)
      }
    } else {
      cont()
    }
  }
  
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) : Unit = {
    val c_id = get_customer_id(state, c_w_id, c_d_id, c)
    if(c_id < 0)
      return
    
    val w = state.warehouses(w_id)
    val d = w.districts(d_id)
    d.synchronized {
      d.ytd += amount
      
      val c_w = state.warehouses(c_w_id)
      val c_d = c_w.districts(c_d_id)
      
      val c = c_d.customers(c_id)
      var next_data = c.data
      if(c.credit == "BC") {
        next_data = (c_id + "," + c_d_id + "," + c_w_id + "," + d_id + "," + w_id + "," + amount + "/" + c.data)
        if(next_data.length() > 50)
          next_data = next_data.substring(0, 50)
      }
      c.balance += amount
      c.ytd_payment += amount
      c.payment_cnt += 1
      c.data = next_data
    
      w.synchronized {
        w.ytd += amount
      }
    }
  }
  
  def order_status(w_id : Int, d_id : Int, c : Either[Int,String]) : Unit = {
    val c_id = get_customer_id(state, w_id, d_id, c)
    if(c_id < 0)
      return
      
    val w = state.warehouses(w_id)
    val d = w.districts(d_id)
    val customer = d.customers(c_id)
    
    d.synchronized {
      if(customer.last_order_id != -1) {
        d.orders.get(customer.last_order_id)
      }
    }
    
    // TODO: read order status?
  }
  
  def delivery(w_id : Int, o_carrier_id : Int) : Boolean = {
    var skipped = false
    for(d_id <- 1 to 10) {
      val w = state.warehouses(w_id)
      
      val ol_delivery_d = Some(System.currentTimeMillis())

      val d = w.districts(d_id)
      
      d.synchronized {
        val it = d.new_orders.iterator
        if(it.hasNext) {
          val o_id = it.next()
          d.new_orders.remove(o_id)
          
          var sum = 0.0
          for(ol <- d.orders(o_id).order_lines) {
            sum += ol.amount
            ol.delivery_d = ol_delivery_d
          }
          
          val c = d.customers(d.orders(o_id).c_id)
          c.balance += sum
          c.delivery_cnt += 1
        } else {
          skipped = true
        }
      }
    }
    skipped
  }
  
  def stock_level(w_id : Int, d_id : Int, threshold : Int) : Int = {
    val w = state.warehouses(w_id)
    val d = w.districts(d_id)
    
    val high = Math.max(d.next_o_id - 1, 1).toInt
    val low = Math.max(high - 20, 1).toInt
   
    val seen = new HashSet[Int]()
    var count = 0
    
    d.synchronized {
      for(i <- low to high) {
        val o = d.orders(i)
        for(ol <- o.order_lines) {
          if(!seen.contains(ol.i_id) && w.stock(ol.i_id).quantity < threshold) {
            count += 1
          }
          seen.add(ol.i_id)
        }
      }
    }
    
    count
  }
  
  private def get_customer_id(db : LockingDatabase, w_id : Int, d_id : Int, c : Either[Int,String]) : Int =
    if(c.isRight) {
      val warehouse = db.warehouses(w_id)
      val district = warehouse.districts(d_id)
      district.customer_name_index.get(c.right.get) match {
        case Some(c_ids) => {
          var buffer = Buffer[(Int,LockingCustomer)]()
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

object LockingTpcc {
  val maxOrders = 3000  // max orders per district, minimum is 3000
  
  def generate(warehouse_count : Int) : LockingDatabase = {
    var items = Map[Int,LockingItem]()
    for(i_id <- 1 to 100000) {
      val item = LockingItem(Util.randa(Util.rand(14, 24)), Util.rand(100, 10000) * 0.01, Util.randOriginal)
      items += (i_id -> item)
    }
    
    var warehouses = Array.ofDim[LockingWarehouse](warehouse_count + 1)
    for(w_id <- 1 to warehouse_count) {
      var stock = Map[Int, LockingStock]()
      for(i_id <- 1 to 100000) {
        stock.put(i_id, LockingStock(Util.rand(10, 100), 0, 0, 0, Util.randOriginal))
      }
      
      var districts = Array.ofDim[LockingDistrict](11)
      for(d_id <- 1 to 10) {
        val district = LockingDistrict(3001, Util.rand(0, 2000) * 0.0001, 30000.0,
          Map[Int, LockingCustomer](),
          Map[String,Set[Int]](),
          Map[Int, LockingOrder](),
          Set[Int]()
        )
        districts(d_id) = district
      }
      
      val warehouse = LockingWarehouse(Util.randa(Util.rand(6, 10)), Util.rand(0, 2000) * 0.0001, 300000.0, districts, stock)
      warehouses(w_id) = warehouse
    }
    
    for(w_id <- 1 to warehouse_count) {
      val w = warehouses(w_id)
      for(d_id <- 1 to 10) {
        val d = w.districts(d_id)
        for(c_id <- 1 to 3000) {
          val first = Util.name(Util.rand(0, 999))
          val last = Util.name(Util.rand(0, 999))
          d.customers.put(c_id, LockingCustomer(first, last, if(Util.rand(0,10) == 0) "BC" else "GC", Util.randa(Util.rand(30, 50)), Util.rand(0, 5000) * 0.0001, -10.0, 10, 1, 0, -1))
          if(!d.customer_name_index.contains(last))
            d.customer_name_index.put(last, Set[Int]())
          d.customer_name_index(last).add(c_id)
        }
        
        var o_id = 1
        for(c_id <- Util.permutation(3000)) {
          val ol_cnt = Util.rand(5, 15)
          var order_lines = Array.ofDim[LockingOrderLine](ol_cnt)
          for(number <- 1 to ol_cnt) {
            order_lines(number - 1) = LockingOrderLine(Util.rand(1, 100000), w_id, Some(System.currentTimeMillis()), 5, 0.00)
          }
          d.orders.put(o_id, LockingOrder(c_id, System.currentTimeMillis(), if(o_id < 2101) Some(Util.rand(1, 10)) else None, ol_cnt, order_lines))
          d.customers(c_id).last_order_id = o_id
          if(o_id >= 2101) {
            d.new_orders.add(o_id)
          }
          o_id += 1
        }
      }
    }
    
    LockingDatabase(warehouses, items)
  }
}
