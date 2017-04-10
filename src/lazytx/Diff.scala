package lazytx

import scala.concurrent.util.Unsafe

import laziness.Lazy
import lazytrie.Context
import lazytrie.Map
import lazytrie.TrieEmpty
import lazytrie.test.Util

abstract class Diff[T] {
  def apply(v : T) : T
  def force(state : T) : Unit
}

class IntDiff extends Diff[Int] {
  var delta = 0
  var commit = -1
  
  def read = {
    val d = delta
    () => { if(commit == -1)
      throw new Exception; commit + d }
  }
  
  def add(amount : Int) = {
    delta += amount
  }
  
  def apply(v : Int) : Int = {    
    commit = v
    v + delta 
  }
  def force(state : Int) = {}
}

class DoubleDiff extends Diff[Double] {
  var delta = 0.0
  var commit = Double.MinValue
  
  def read = {
    val d = delta
    () => { if(commit == Double.MinValue)
      throw new Exception; commit + d }
  }
  
  def add(amount : Double) = {
    delta += amount
  }
  
  def apply(v : Double) : Double = {
    commit = v
    v + delta 
  }
  def force(state : Double) = {}
}

object MapDiff {
  val offset = Unsafe.instance.objectFieldOffset(classOf[MapDiff[_,_]].getDeclaredField("applied"))
  
  def apply[K,V](map : Map[K,V]) = new MapDiff(map.emptyMap[Option[V] => Option[V]].context)
}

class MapDiff[K,V](context : Context[K,Option[V] => Option[V]]) extends Diff[Map[K,V]] {
  var snapshot : Map[K,V] = null
  var applied : Map[K,V] = null
  var diff : Map[K,Option[V] => Option[V]] = new Map(context, TrieEmpty.instance)
 
  def empty = new MapDiff[K,V](context)
  
  def update(k : K, f : Option[V] => Option[V]) : Unit = {
    diff(k) = (x : Option[Option[V] => Option[V]]) => x match {
      case Some(current) => Some(f compose current)
      case None => Some(f)
    }
  }
  
  def apply(k : K, f : V => V) : Unit =
    update(k, (ov : Option[V]) => ov.map(f))
    
  def update(k : K, f : Option[V] => Option[V], condition : Lazy[Boolean]) : Unit =
    update(k, (v : Option[V]) => if(condition.get) f(v) else v)
  
  def read(k : K) : () => Option[V] = {
    if(diff.get(k) == None) {
      () => snapshot.get(k)
    } else {
      var r : Option[V] = null
      
      update(k, c => {
        r = c
        c
      })
      
      () => {
        if(r == null)
          applied.force(k)
        r
      }
    }
  }
  
  def apply(map : Map[K,V]) : Map[K,V] = {
    if(applied == null) {
      snapshot = map
      val next_applied = map.update(diff)
      if(Unsafe.instance.compareAndSwapObject(this, MapDiff.offset, null, next_applied))
        next_applied
      else
        applied  
    } else
      applied
  }
  
  def force(state : Map[K,V]) = {
    state.force(diff)
    //diff = null    // Seems to be expensive
  }
}

/*
class ArrayMapDiff[K,V](context : Context[K,ArrayBuffer[Option[V] => Option[V]]]) extends MapDiff[K,V] {
  var snapshot : Map[K,V] = null
  var applied : Map[K,V] = null
  var diff : Map[K,ArrayBuffer[Option[V] => Option[V]]] = new Map(context, TrieEmpty.instance)
 
  def empty = new ArrayMapDiff[K,V](context)
  
  def update(k : K, f : Option[V] => Option[V]) : Unit = {
    diff.get(k) match {
      case Some(current) => { current += f }
      case None => diff(k) = ArrayBuffer(f)
    }
  }
  
  def read(k : K) : () => Option[V] = {
    if(diff.get(k) == None) {
      () => snapshot.get(k)
    } else {
      var r : Option[V] = null
      
      update(k, c => {
        r = c
        c
      })
      
      () => {
        if(r == null)
          applied.get(k)
        r
      }
    }
  }
  
  def eval(init : Option[V], ab : ArrayBuffer[Option[V] => Option[V]]) : Option[V] = {
    var current = init
    var i = 0
    while(i < ab.length) {
      current = ab(i)(current)
      i += 1
    }
    current
  }
  
  def apply(map : Map[K,V]) : Map[K,V] = {
    snapshot = map
    val a = Some((k : K, v : ArrayBuffer[Option[V] => Option[V]]) => eval(None, v))
    val b = (k : K, v : V, f : ArrayBuffer[Option[V] => Option[V]]) => eval(Some(v), f)
    
    applied = map.mergeKD(diff, None, a, b)
    applied
  }
  
  def force = {
    snapshot.forceRoot
    applied.forceRoot
    applied.force(diff)
    diff = null
  }
}
*/

/*
class MapDiff[K,V](context : Context[K,Lazy[Option[V]]]) extends Diff[Map[K,V]] {
  var last = new Map(context, TrieEmpty.instance)
  var commit : Map[K,V] = null
  var result : Map[K,V] = null
 
  def empty = new MapDiff[K,V](context)
  
  def update(k : K, f : () => Option[V]) = {
    last(k) = Lazy.optimistic(f)
  }
  
  def update(k : K, f : Option[V] => Option[V]) = {
    last(k) = _ match {
      case None => Some(Lazy.optimistic(() => f(commit.get(k))))
      case Some(last) => Some(Lazy.optimistic(() => f(last.get)))
    }
  }
  
  def apply(k : K, f : V => V) : Unit =
    update(k, (ov : Option[V]) => ov.map(f))
    
  def update(k : K, f : Option[V] => Option[V], condition : Lazy[Boolean]) : Unit =
    update(k, (v : Option[V]) => if(condition.get) f(v) else v)
  
  def read(k : K) : () => Option[V] = {
    last.get(k) match {
      case None => () => commit.get(k)
      case Some(last) => () => last.get
    }
  }
  
  def apply(map : Map[K,V]) : Map[K,V] = {
    val a = Some((k : K, v : Lazy[Option[V]]) => v.get)
    val b = (k : K, v : V, f : Lazy[Option[V]]) => f.get
    
    commit = map
    result = map.mergeKD(last, None, a, b)
    result
  }
  
  def force = {
    result.force(last)
    last = null
    result = null
  }
}
*/

object DiffTest {
  def main(args : Array[String]) = {
    val state = new State(Util.createIntMap(10, 2))
    val diff = state.get.diff
    
    diff.apply(0, _ + 1)
    val f0 = diff.read(1)
    println(state.get)
    state.update(diff.apply(_))
    println(f0())
    println(state.get)
  }
}