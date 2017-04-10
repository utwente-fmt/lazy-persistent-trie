package lazytx.benchmark.exchange

import lazytrie.Map
import lazytrie.SmallIntKey
import lazytx.State

abstract class ExchangeApi {
  def limitBuyOrder(c_id : Int, volume : Int, price : Int) : (Int, Int)   // returns (price, handle)
  def limitSellOrder(c_id : Int, volume : Int, price : Int) : (Int, Int)  // returns (price, handle)
  def cancelBuyOrder(handle : Int)
  def cancelSellOrder(handle : Int)
  def orderStatus(c_id : Int)
  def orderBook()
}

// to be efficient, probably needs a custom data structure that indexes cumulative volume in nodes
case class LazyExchange() {
  // c_id -> (money balance, stock balance)
  val customers = Map[Int, (Int, Int)]()(new SmallIntKey(1000))
   
  // price -> (c_id, count)
  val asks = Map[Int, List[(Int, Int)]]()(new SmallIntKey(10000))
  val bids = Map[Int, List[(Int, Int)]]()(new SmallIntKey(10000))
}
 
/*
class LazyExchangeApi(state : State[LazyExchange]) extends ExchangeApi {
  // idea: just map over asks, but abort early when order is filled
  // idea: use an index to find up to where an order could match
  // idea: just insert orders, and match them when reading (basically a two-step process)
    // that is, when reading the ask book, check the bids book to see if asks have been matched
  def limitBuyOrder(c_id : Int, volume : Int, limit : Int) : Int = {
    state.update(e => {
      //e.bids.modify(limit, os => os + (c_id, count))
      e
    })
    
    0
  }
  
  def limitSellOrder(c_id : Int, volume : Int, limit : Int) : Int = {
    0
  }
  
  def cancelBuyOrder(handle : Int) = {
    
  }
  
  def cancelSellOrder(handle : Int) = {
    
  }
  
  def orderStatus(c_id : Int) = {
    
  }
  
  def orderBook() = {
    
  }
}
*/
