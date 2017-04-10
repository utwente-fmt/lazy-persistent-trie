package lazytx.benchmark.tpcc

import scala.concurrent.stm.TMap
import scala.concurrent.stm.TSet
import scala.concurrent.stm.atomic
import scala.concurrent.stm.Ref
import scala.concurrent.stm.InTxn
import java.util.HashSet
import scala.collection.mutable.Buffer
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.stm.TArray
import scala.collection.mutable.Map
import scala.collection.mutable.Set

case class StmWarehouse(name : String, tax : Double, ytd : Ref[Double], districts : Array[StmDistrict], stock : Map[Int, StmStock])
case class StmDistrict(next_o_id : Ref[Int], tax : Double, ytd : Ref[Double], customers : Map[Int, StmCustomer], customer_name_index : Map[String, Set[Int]], orders : TMap[Int, StmOrder], new_orders : TSet[Int])
case class StmCustomer(first : String, last : String, credit : String, data : Ref[String], discount : Double, balance : Ref[Double], ytd_payment : Ref[Double], payment_cnt : Ref[Int], delivery_cnt : Ref[Int], last_order_id : Ref[Int])
case class StmOrder(c_id : Int, entry_d : Long, carrier_id : Option[Int], ol_cnt : Int, order_lines : Array[StmOrderLine])
case class StmOrderLine(i_id : Int, supply_w_id : Int, delivery_d : Ref[Option[Long]], quantity : Int, amount : Double)
case class StmItem(name : String, price : Double, data : String)
case class StmStock(quantity : Ref[Int], ytd : Ref[Int], order_cnt : Ref[Int], remote_cnt : Ref[Int], data : String)
case class StmDatabase(warehouses : Array[StmWarehouse], items : Map[Int, StmItem])

class StmTpcc(val state : StmDatabase, warehouseCount : Int) extends Tpcc {
  def new_order(w_id : Int, d_id : Int, c_id : Int, item_orders : Array[ItemOrder]) : Double = {  
    val warehouse = state.warehouses(w_id)
    val district = warehouse.districts(d_id)
    val customer = district.customers(c_id)
    
    var sum_amount = 0.0
    var ol_id = 0
    val order_lines = Array.ofDim[StmOrderLine](item_orders.length)
    item_orders.foreach(io => {
      val item = state.items.get(io.i_id)
      if(item.isEmpty)
        return -1
      val amount = item.get.price * io.quantity
      order_lines(ol_id) = StmOrderLine(io.i_id, io.supply_w_id, Ref(None), io.quantity, amount)
      val bg = item.get.data.contains("ORIGINAL") && state.warehouses(io.supply_w_id).stock(io.i_id).data.contains("ORIGINAL")
      sum_amount += amount
      ol_id += 1
    })
    val total_amount = sum_amount * (1 - customer.discount) * (1 + warehouse.tax + district.tax)
    
    val carrier_id = Some(ThreadLocalRandom.current().nextInt(10))
    val now = System.currentTimeMillis()
  
    atomic { implicit txn =>
      val o_id = district.next_o_id.get % StmTpcc.maxOrders
      
      // Add order
      district.orders(o_id) = StmOrder(c_id, now, carrier_id, item_orders.length, order_lines) 
      district.new_orders.add(o_id)
      district.customers(c_id).last_order_id.set(o_id)
      district.next_o_id.set((district.next_o_id.get + 1) % StmTpcc.maxOrders)
      
      // Update stock
      item_orders.foreach(io => {
        val supply_w = state.warehouses(io.supply_w_id)

        val s = supply_w.stock(io.i_id)        
        val q = s.quantity.get
        
        s.quantity.set(if(q >= io.quantity + 10) q - io.quantity else q - io.quantity + 91)
        s.ytd.set(s.ytd.get + io.quantity)
        s.order_cnt.set(s.order_cnt.get + 1)
        if(io.supply_w_id != w_id)
          s.remote_cnt.set(s.remote_cnt.get + 1)
      })
    }
    
    total_amount
  }
  
  def payment(w_id : Int, d_id : Int, c_w_id : Int, c_d_id : Int, c : Either[Int,String], amount : Double) : Unit = {
    val c_id = get_customer_id(state, c_w_id, c_d_id, c)
    if(c_id < 0)
      return
    
    val w = state.warehouses(w_id)
    val d = w.districts(d_id)
    val c_w = state.warehouses(c_w_id)
    val c_d = c_w.districts(c_d_id)
    val cust = c_d.customers(c_id)
      
    atomic { implicit txn =>  
      w.ytd.set(w.ytd.get + amount)
      d.ytd.set(d.ytd.get + amount)
      
      if(cust.credit == "BC") {
        var next_data = cust.data.get
        next_data = (c_id + "," + c_d_id + "," + c_w_id + "," + d_id + "," + w_id + "," + amount + "/" + cust.data.get)
        if(next_data.length() > 50)
          next_data = next_data.substring(0, 50)
        cust.data.set(next_data)
      }
      
      cust.balance.set(cust.balance.get + amount)
      cust.ytd_payment.set(cust.ytd_payment.get + amount)
      cust.payment_cnt.set(cust.payment_cnt.get + 1)
    }
  }
  
  def order_status(w_id : Int, d_id : Int, c : Either[Int,String]) : Unit = {  
    val c_id = get_customer_id(state, w_id, d_id, c)
    if(c_id < 0)
      return
      
    val w = state.warehouses(w_id)
    val d = w.districts(d_id)
    val customer = d.customers(c_id)
  
    atomic { implicit txn =>
      if(customer.last_order_id.get != -1) {
        d.orders.get(customer.last_order_id.get)
      }
    }
  }
  
  def delivery(w_id : Int, o_carrier_id : Int) : Boolean = {
    var skipped = false
    for(d_id <- 1 to 10) {
      val w = state.warehouses(w_id)
      val d = w.districts(d_id)
      
      val ol_delivery_d = Some(System.currentTimeMillis())
      
      atomic { implicit txn =>
        val it = d.new_orders.iterator
        if(it.hasNext) {
          val o_id = it.next()
          d.new_orders.remove(o_id)
          
          var sum = 0.0
          for(ol <- d.orders(o_id).order_lines) {
            sum += ol.amount
            ol.delivery_d.set(ol_delivery_d)
          }
          
          val c_id = d.orders(o_id).c_id
          val c = d.customers(c_id)
          c.balance.set(c.balance.get + sum)
          c.delivery_cnt.set(c.delivery_cnt.get + 1)
        } else {
          skipped = true
        }
      }
    }
    skipped
  }
  
  def stock_level(w_id : Int, d_id : Int, threshold : Int) : Int = {
    atomic { implicit txn =>
      val w = state.warehouses(w_id)
      val d = w.districts(d_id)
      
      val high = Math.max(d.next_o_id.get - 1, 1).toInt
      val low = Math.max(high - 20, 1).toInt
     
      val seen = new HashSet[Int]()
      var count = 0
      
      for(i <- low to high) {
        val o = d.orders(i)
        for(ol <- o.order_lines) {
          if(!seen.contains(ol.i_id) && w.stock(ol.i_id).quantity.get < threshold) {
            count += 1
          }
          seen.add(ol.i_id)
        }
      }
      
      count
    }
  }
  
  private def get_customer_id(db : StmDatabase, w_id : Int, d_id : Int, c : Either[Int,String]) : Int =
    if(c.isRight) {
      val warehouse = db.warehouses(w_id)
      val district = warehouse.districts(d_id)
      district.customer_name_index.get(c.right.get) match {
        case Some(c_ids) => {
          var buffer = Buffer[(Int,StmCustomer)]()
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

object StmTpcc {
  val maxOrders = 3000  // max orders per district, minimum is 3000
  
  def generate(warehouse_count : Int) : StmDatabase = {
    atomic { implicit txn =>
      var items = Map[Int,StmItem]()
      for(i_id <- 1 to 100000) {
        val item = StmItem(Util.randa(Util.rand(14, 24)), Util.rand(100, 10000) * 0.01, Util.randOriginal)
        items += (i_id -> item)
      }
      
      var warehouses = Array.ofDim[StmWarehouse](warehouse_count + 1)
      for(w_id <- 1 to warehouse_count) {
        var stock = Map[Int, StmStock]()
        for(i_id <- 1 to 100000) {
          stock.put(i_id, StmStock(Ref(Util.rand(10, 100)), Ref(0), Ref(0), Ref(0), Util.randOriginal))
        }
        
        var districts = Array.ofDim[StmDistrict](11)
        for(d_id <- 1 to 10) {
          val district = StmDistrict(Ref(3001), Util.rand(0, 2000) * 0.0001, Ref(30000.0),
            Map[Int, StmCustomer](),
            Map[String,Set[Int]](),
            TMap[Int, StmOrder](),
            TSet[Int]()
          )
          districts(d_id) = district
        }
        
        val warehouse = StmWarehouse(Util.randa(Util.rand(6, 10)), Util.rand(0, 2000) * 0.0001, Ref(300000.0), districts, stock)
        warehouses(w_id) = warehouse
      }
      
      for(w_id <- 1 to warehouse_count) {
        val w = warehouses(w_id)
        for(d_id <- 1 to 10) {
          val d = w.districts(d_id)
          for(c_id <- 1 to 3000) {
            val first = Util.name(Util.rand(0, 999))
            val last = Util.name(Util.rand(0, 999))
            d.customers(c_id) = StmCustomer(first, last, if(Util.rand(0,10) == 0) "BC" else "GC", Ref(Util.randa(Util.rand(30, 50))), Util.rand(0, 5000) * 0.0001, Ref(-10.0), Ref(10), Ref(1), Ref(0), Ref(-1))
            if(!d.customer_name_index.contains(last))
              d.customer_name_index.put(last, TSet[Int]())
            d.customer_name_index(last).add(c_id)
          }
          
          var o_id = 1
          for(c_id <- Util.permutation(3000)) {
            val ol_cnt = Util.rand(5, 15)
            var order_lines = Array.ofDim[StmOrderLine](ol_cnt)
            for(number <- 0 to ol_cnt - 1) {
              order_lines(number) = StmOrderLine(Util.rand(1, 100000), w_id, Ref(Some(System.currentTimeMillis())), 5, 0.00)
            }
            d.orders.put(o_id, StmOrder(c_id, System.currentTimeMillis(), if(o_id < 2101) Some(Util.rand(1, 10)) else None, ol_cnt, order_lines))
            d.customers(c_id).last_order_id.set(o_id)
            if(o_id >= 2101) {
              d.new_orders.add(o_id)
            }
            o_id += 1
          }
        }
      }
      
      StmDatabase(warehouses, items)
    }
  }
}