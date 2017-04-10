package lazytx.benchmark.transform

import lazytx.State


case class OrderLine(i_id : Int, quantity : Int, amount : Double)
case class Order(carrier_id : Option[Int], order_lines : Map[Int, OrderLine])
case class Database(orders : Map[Int, Order], next_order_id : Int)

class Workload(state : State[Database]) {
  //def order_status(o_id)
}

// ostat(o_id)
// new_order(items)
// delivery

class Relational {

}
