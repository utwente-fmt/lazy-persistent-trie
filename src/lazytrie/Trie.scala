package lazytrie

import java.util.LinkedList
import laziness.OptimisticThunk
import laziness.Lazy
import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom

case class Context[K,V](
  branchWidth : Int,
  key : Key[K],
  emptyBranch : TrieBranch[K,V],
  offset : Int
)

abstract class TrieNode[K,V] extends Lazy[TrieNode[K,V]] {
  def get(context : Context[K,V], offset : Int, k : K) : Option[V]
  def floor(context : Context[K,V], offset : Int, k : K) : Option[(K,V)]
  def ceil(context : Context[K,V], offset : Int, k : K) : Option[(K,V)]
  def prev(context : Context[K,V], offset : Int, k : K) : Option[(K,V)]
  def next(context : Context[K,V], offset : Int, k : K) : Option[(K,V)]
  def first : Option[(K,V)]
  def last : Option[(K,V)]
  
  def foreach(f : (K,V) => Unit) : Unit
  
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V]
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V]
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V]
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V]
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V]
  
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : Lazy[TrieNode[K,V]]
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) : Lazy[TrieNode[K,V]]
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : Lazy[TrieNode[K,V]]
  
  def map[W](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) : TrieNode[K,W]
  
  def mergeDD[W,X](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], cx : Context[K,X], offset : Int, left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) : TrieNode[K,X]
  def mergeKD[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) : TrieNode[K,V]
  def mergeDK[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) : TrieNode[K,W]
  def mergeKK(other : Lazy[TrieNode[K,V]], c1 : Context[K,V], c2 : Context[K,V], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) : TrieNode[K,V]
  
  def removeFrom(context : Context[K,V], offset : Int, low : K, including : Boolean) : TrieNode[K,V]
  def removeUpto(context : Context[K,V], offset : Int, high : K, including : Boolean) : TrieNode[K,V]
  def removeRange(context : Context[K,V], offset : Int, low : K, high : K, lowIncluding : Boolean, highIncluding : Boolean) : TrieNode[K,V]
  
  def updateRange(context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) : TrieNode[K,V]
  def updateFrom(context : Context[K,V], offset : Int, f : V => Option[V], low : K) : TrieNode[K,V]
  def updateUpto(context : Context[K,V], offset : Int, f : V => Option[V], high : K) : TrieNode[K,V]
  def updateAll(context : Context[K,V], offset : Int, f : V => Option[V]) : TrieNode[K,V]
  
  def replaceRange(context : Context[K,V], offset : Int, f : V => V, low : K, high : K) : TrieNode[K,V]
  def replaceFrom(context : Context[K,V], offset : Int, f : V => V, low : K) : TrieNode[K,V]
  def replaceUpto(context : Context[K,V], offset : Int, f : V => V, high : K) : TrieNode[K,V]
  def replaceAll(context : Context[K,V], offset : Int, f : V => V) : TrieNode[K,V]
  
  def reduceRange[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K, high : K) : T
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T
  def reduceAll[T](accum : T, f : (T,V) => T) : T
  
  def compact(context : Context[K,V]) : TrieNode[K,V]
  
  def force(context : Context[K,V], offset : Int, k : K) : Unit
  def forceRange(context : Context[K,V], offset : Int, low : K, high : K) : Unit
  def forceFrom(context : Context[K,V], offset : Int, low : K) : Unit
  def forceUpto(context : Context[K,V], offset : Int, high : K) : Unit
  def forceAll() : Unit
  def forceKeys(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) : Unit
  def forceBranches(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) : Unit
  
  def printDebug : String
}

object TrieEmpty {
  val _instance = TrieEmpty[Nothing,Nothing]
  @inline def instance[K,V] : TrieEmpty[K,V] = _instance.asInstanceOf[TrieEmpty[K,V]]
}

case class TrieEmpty[K,V]() extends TrieNode[K,V] {
  def get = this
  def evaluate = this
  def isEvaluated = true
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = None
  def floor(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = None
  def ceil(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = None
  def prev(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = None
  def next(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = None
  def first : Option[(K,V)] = None
  def last : Option[(K,V)] = None
  
  def foreach(f : (K,V) => Unit) : Unit = {}
  
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] =
    TrieLeaf(k,v)
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V] =
    if(condition.get) put(context, offset, k, v) else this
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) = 
    TrieLeaf(k,v)
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) = f(None) match {
    case Some(v) => TrieLeaf(k,v)
    case None => this
  }
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] =
    update(context, offset, k, f)
  
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : Lazy[TrieNode[K,V]] = diff
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) =
    if(condition.get) write(context, offset, diff) else this
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : Lazy[TrieNode[K,V]] =
    diff.get.map(dcontext, offset, (k : K, f : Option[V] => Option[V]) => f(None))
    
  def map[W](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) = TrieEmpty.instance
  
  // TODO: the following should map lazily
  def mergeDD[W,X](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], cx : Context[K,X], offset : Int, left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) : TrieNode[K,X] =
    right match {
      case Some(f) => other.get.map(c2, offset, f)
      case None => this.asInstanceOf[TrieNode[K,X]]
    }
      
  def mergeKD[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) : TrieNode[K,V] =
    right match {
      case Some(f) => other.get.map(c2, offset, f)
      case None => this
    }
  
  def mergeDK[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) : TrieNode[K,W] =
    other.get
    
  def mergeKK(other : Lazy[TrieNode[K,V]], c1 : Context[K,V], c2 : Context[K,V], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) : TrieNode[K,V] =
    right match {
      case Some(f) => other.get.map(c2, offset, f)
      case None => other.get
    }
  
  def removeRange(context : Context[K,V], offset : Int, low : K, high : K, lowIncluding : Boolean, highIncluding : Boolean): TrieNode[K,V] = this
  def removeFrom(context : Context[K,V], offset : Int, low : K, including : Boolean): TrieNode[K,V] = this
  def removeUpto(context : Context[K,V], offset : Int, high : K, including : Boolean): TrieNode[K,V] = this

  def updateRange(context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) = this
  def updateFrom(context : Context[K,V], offset : Int, f : V => Option[V], low : K) = this
  def updateUpto(context : Context[K,V], offset : Int, f : V => Option[V], high : K) = this
  def updateAll(context : Context[K,V], offset : Int, f : V => Option[V]) : TrieNode[K,V] = this
  
  def replaceRange(context : Context[K,V], offset : Int, f : V => V, low : K, high : K) = this
  def replaceFrom(context : Context[K,V], offset : Int, f : V => V, low : K) = this
  def replaceUpto(context : Context[K,V], offset : Int, f : V => V, high : K) = this
  def replaceAll(context : Context[K,V], offset : Int, f : V => V) : TrieNode[K,V] = this
  
  def reduceRange[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K, high : K) : T = accum
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T = accum
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T = accum
  def reduceAll[T](accum : T, f : (T,V) => T) : T = accum

  def compact(context : Context[K,V]) = this
  
  def force(context : Context[K,V], offset : Int, k : K) : Unit = {}
  def forceRange(context : Context[K,V], offset : Int, low : K, high : K) : Unit = {}
  def forceFrom(context : Context[K,V], offset : Int, low : K) : Unit = {}
  def forceUpto(context : Context[K,V], offset : Int, high : K) : Unit = {}
  def forceAll() : Unit = {}
  def forceKeys(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = {}
  def forceBranches(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = {}
  
  def printDebug = "E"
}

case class TrieLeaf[K,V](key : K, var value : V) extends TrieNode[K,V] {
  def get = this
  def evaluate = this
  def isEvaluated = true
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = if(context.key.eq(k, key)) Some(value) else None
  def floor(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] =
    if(context.key.compare(key, k) <= 0)
      Some((key, value))
    else
      None
  def ceil(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] =
    if(context.key.compare(key, k) >= 0)
      Some((key, value))
    else
      None
  def prev(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] =
    if(context.key.compare(key, k) < 0)
      Some((key, value))
    else
      None
  def next(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] =
    if(context.key.compare(key, k) > 0)
      Some((key, value))
    else
      None
  def first : Option[(K,V)] = Some((key, value))
  def last : Option[(K,V)] = Some((key, value))
  
  def foreach(f : (K,V) => Unit) : Unit = f(key, value)
  
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] =
    if(context.key.eq(k, key)) {
      TrieLeaf(key, v)
    } else {
      val r = context.emptyBranch.copy
      r.putInPlace(context, offset, key, value)
      r.putInPlace(context, offset, k, v)
      r
    }
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V] =
    if(condition.get) put(context, offset, k, v) else this
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = {
    if(context.key.eq(k, key)) {
      value = v
      this
    } else {
      val r = context.emptyBranch.copy
      r.putInPlace(context, offset, key, value)
      r.putInPlace(context, offset, k, v)
      r
    }
  }
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) = {
    if(context.key.eq(k, key)) {
      f(Some(value)) match {
        case Some(v) => TrieLeaf(key, v)
        case None => TrieEmpty.instance
      }
    } else {
      val r = context.emptyBranch.copy
      r.putInPlace(context, offset, key, value)
      r.updateInPlace(context, offset, k, f)
      r
    }
  }
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = {
    if(context.key.eq(k, key)) {
      f(Some(value)) match {
        case None => TrieEmpty.instance
        case Some(v) =>
          value = v
          this
      }
    } else {
      val r = context.emptyBranch.copy
      r.putInPlace(context, offset, key, value)
      r.updateInPlace(context, offset, k, f)
      r
    }
  }
  
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : TrieNode[K,V] =
    diff.get.update(context, offset, key, (v : Option[V]) => v match { case None => Some(value) case v => v })
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) =
    if(condition.get) write(context, offset, diff) else this
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : TrieNode[K,V] = diff.get match {
    case TrieLeaf(k,f) => update(context, offset, k, f)
    case other => 
      other.update(dcontext, offset, key, _ match {
        case None => Some(_ => Some(value))
        case Some(f) => Some(_ => f(Some(value)))
      }).map(dcontext, offset, (k, f) => f(None))
  }
  
  def map[W](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) = f(key, value) match {
    case Some(v) => TrieLeaf(key, v)
    case None => TrieEmpty.instance
  }
  
  def mergeDD[W,X](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], cx : Context[K,X], offset : Int, left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) : TrieNode[K,X] = other.get match {
    case TrieLeaf(k,v) => if(c1.key.eq(key, k)) {
      merge(key, value, v) match {
        case Some(v) => TrieLeaf(k, v)
        case None => TrieEmpty._instance.asInstanceOf[TrieNode[K,X]]
      }
    } else {
      var r = TrieEmpty._instance.asInstanceOf[TrieNode[K,X]]
      if(left.isDefined)
        r = r.updateInPlace(cx, offset, key, _ => left.get(key, value))
      if(right.isDefined)
        r = r.updateInPlace(cx, offset, k, _ => right.get(k, v))
      r
    }
    case other => other.mergeDD(this, c2, c1, cx, offset, right, left, (k,w,v : V) => merge(k,v,w))
  }
  
  def mergeKD[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) : TrieNode[K,V] = other.get match {
    case TrieLeaf(k,v) => if(c1.key.eq(key, k)) {
      merge(key, value, v) match {
        case Some(v) => TrieLeaf(k, v)
        case None => TrieEmpty.instance
      }
    } else {
      var r = TrieEmpty._instance.asInstanceOf[TrieNode[K,V]]
      if(left.isDefined)
        r = r.updateInPlace(c1, offset, key, _ => left.get(key, value))
      else
        r = this 
      if(right.isDefined)
        r = r.updateInPlace(c1, offset, k, _ => right.get(k, v))
      r
    }
    case other => other.mergeDK(this, c2, c1, offset, right, left, (k : K, w : W, v : V) => merge(k,v,w))
  }
  
  def mergeDK[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) : TrieNode[K,W] =  other.get match {
    case t@TrieLeaf(k,v) => if(c1.key.eq(key, k)) {
      merge(key, value, v) match {
        case Some(v) => TrieLeaf(k, v)
        case None => TrieEmpty._instance.asInstanceOf[TrieNode[K,W]]
      }
    } else {
      var r = TrieEmpty._instance.asInstanceOf[TrieNode[K,W]]
      if(right.isDefined)
        r = r.updateInPlace(c2, offset, k, _ => right.get(k, v))
      else
        r = t
      if(left.isDefined)
        r = r.updateInPlace(c2, offset, key, _ => left.get(key, value))
      r
    }
    case other => other.mergeKD(this, c2, c1, offset, right, left, (k : K, w : W, v : V) => merge(k,v,w))
  }
    
  def mergeKK(other : Lazy[TrieNode[K,V]], c1 : Context[K,V], c2 : Context[K,V], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) : TrieNode[K,V] = other.get match {
    case TrieLeaf(k,v) => if(c1.key.eq(key, k)) {
      merge(key, value, v) match {
        case Some(v) => TrieLeaf(k, v)
        case None => TrieEmpty._instance.asInstanceOf[TrieNode[K,V]]
      }
    } else {
      var r = TrieEmpty._instance.asInstanceOf[TrieNode[K,V]]
      if(left.isDefined)
        r = r.updateInPlace(c1, offset, key, _ => left.get(key, value))
      else
        r = this 
      if(right.isDefined)
        r = r.updateInPlace(c1, offset, k, _ => right.get(k, v))
      else
        r = r.updateInPlace(c1, offset, k, _ => Some(v))
      r
    }
    case other => other.mergeKK(this, c2, c1, offset, right, left, (k : K, w : V, v : V) => merge(k,v,w))
  }  
    
  def removeFrom(context : Context[K,V], offset : Int, low : K, including : Boolean): TrieNode[K,V] =
    if((including && context.key.compare(key, low) > 0) || (!including && context.key.compare(key, low) >= 0))
      this
    else
      TrieEmpty._instance.asInstanceOf[TrieNode[K,V]]
    
  def removeUpto(context : Context[K,V], offset : Int, high : K, including : Boolean): TrieNode[K,V] =
    if((including && context.key.compare(key, high) < 0) || (!including && context.key.compare(key, high) <= 0))
      this
    else
      TrieEmpty._instance.asInstanceOf[TrieNode[K,V]]
  
  def removeRange(context : Context[K,V], offset : Int, low : K, high : K, lowIncluding : Boolean, highIncluding : Boolean): TrieNode[K,V] =
    if(((lowIncluding && context.key.compare(key, low) > 0) || (!lowIncluding && context.key.compare(key, low) >= 0)) && 
        (highIncluding && context.key.compare(key, high) < 0) || (!highIncluding && context.key.compare(key, high) <= 0))
      this
    else
      TrieEmpty._instance.asInstanceOf[TrieNode[K,V]]
  
  def updateRange(context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) =
    if(context.key.compare(key, low) >= 0 && context.key.compare(key, high) <= 0)
      f(value) match {
        case Some(v) => TrieLeaf(key, v)
        case None => TrieEmpty.instance
      }
    else
      this
  def updateFrom(context : Context[K,V], offset : Int, f : V => Option[V], low : K) =
    if(context.key.compare(key, low) >= 0)
      f(value) match {
        case Some(v) => TrieLeaf(key, v)
        case None => TrieEmpty.instance
      }
    else
      this
  def updateUpto(context : Context[K,V], offset : Int, f : V => Option[V], high : K) =
    if(context.key.compare(key, high) <= 0)
      f(value) match {
        case Some(v) => TrieLeaf(key, v)
        case None => TrieEmpty.instance
      }
    else
      this
  def updateAll(context : Context[K,V], offset : Int, f : V => Option[V]) =
    f(value) match {
      case Some(v) => TrieLeaf(key, v)
      case None => TrieEmpty.instance
    }
  
  def replaceRange(context : Context[K,V], offset : Int, f : V => V, low : K, high : K) =
    if(context.key.compare(key, low) >= 0 && context.key.compare(key, high) <= 0)
      TrieLeaf(key, f(value))
    else
      this
  def replaceFrom(context : Context[K,V], offset : Int, f : V => V, low : K) =
    if(context.key.compare(key, low) >= 0)
      TrieLeaf(key, f(value))
    else
      this
  def replaceUpto(context : Context[K,V], offset : Int, f : V => V, high : K) =
    if(context.key.compare(key, high) <= 0)
      TrieLeaf(key, f(value))
    else
      this
  def replaceAll(context : Context[K,V], offset : Int, f : V => V) =
    TrieLeaf(key, f(value))
     
  def reduceRange[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K, high : K) : T =
    if(context.key.compare(key, low) >= 0 && context.key.compare(key, high) <= 0)
      f(accum, value)
    else
      accum
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T =
    if(context.key.compare(key, low) >= 0)
      f(accum, value)
    else
      accum
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T =
    if(context.key.compare(key, high) <= 0)
      f(accum, value)
    else
      accum
  def reduceAll[T](accum : T, f : (T,V) => T) : T = f(accum, value)

  def compact(context : Context[K,V]) = this
  
  def force(context : Context[K,V], offset : Int, k : K) : Unit = {}
  def forceRange(context : Context[K,V], offset : Int, low : K, high : K) : Unit = {}
  def forceFrom(context : Context[K,V], offset : Int, low : K) : Unit = {}
  def forceUpto(context : Context[K,V], offset : Int, high : K) : Unit = {}
  def forceAll() : Unit = {}
  def forceKeys(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = {}
  def forceBranches(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = {}
  
  def printDebug = key + " -> " + value
}

object TrieBranch {
  @inline def findBin[K,V](context : Context[K,V], offset : Int, k : K) =
    if(offset < 0)
    	context.key.partition(k, 0, context.branchWidth + offset)
    else
      context.key.partition(k, offset, context.branchWidth)
  
}

case class TrieBranch[K,V](children : Array[Lazy[TrieNode[K,V]]]) extends TrieNode[K,V] {
  def get = this
  def evaluate = this
  def isEvaluated = true
  
  @inline def copy = TrieBranch(children.clone())
  @inline def as[W] = this.asInstanceOf[TrieBranch[K,W]]
  
  @inline private def getChild(bin : Int) = {
    val c = children(bin)
    val ec = c.get
    if(!(c eq ec)) {
      children(bin) = ec
    }
    ec
  }
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = {
    val bin = TrieBranch.findBin(context, offset, k)
    children(bin).get.get(context, offset + context.branchWidth, k)
  }
  
  def floor(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = {
    var result : Option[(K,V)] = None
    var i = TrieBranch.findBin(context, offset, k)
    result = children(i).get.floor(context, offset + context.branchWidth, k)
    while(i > 0 && result == None) {
      i -= 1
      result = children(i).get.last
    }
    result
  }
  
  def ceil(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = {
    var result : Option[(K,V)] = None
    var i = TrieBranch.findBin(context, offset, k)
    result = children(i).get.ceil(context, offset + context.branchWidth, k)
    i += 1
    while(i < children.length && result == None) {
      result = children(i).get.first
      i += 1
    }
    result
  }
    
  def prev(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = {
    var result : Option[(K,V)] = None
    var i = TrieBranch.findBin(context, offset, k)
    result = children(i).get.prev(context, offset + context.branchWidth, k)
    while(i >= 0 && result == None) {
      i -= 1
      result = children(i).get.last
    }
    result
  }
  
  def next(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = {
    var result : Option[(K,V)] = None
    var i = TrieBranch.findBin(context, offset, k)
    result = children(i).get.next(context, offset + context.branchWidth, k)
    i += 1
    while(i < children.length && result == None) {
      result = children(i).get.first
      i += 1
    }
    result
  }
  
  def first : Option[(K,V)] = {
    var result : Option[(K,V)] = None
    var i = 0
    while(i < children.length && result == None) {
      result = children(i).get.first
      i += 1
    }
    result
  }
  
  def last : Option[(K,V)] = {
    var result : Option[(K,V)] = None
    var i = children.length
    while(i > 0 && result == None) {
      i -= 1
      if(!(children(i) eq TrieEmpty._instance))
        result = children(i).get.last
    }
    result
  }
  
  def foreach(f : (K,V) => Unit) : Unit = {
    var i = children.length
    while(i > 0) {
      i -= 1
      children(i).get.foreach(f) 
    }
  }
  
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    val bin = TrieBranch.findBin(context, offset, k)
    nchildren(bin) = new OptimisticThunk(new PutValueLazy(nchildren(bin), context, offset + context.branchWidth, k, v))
    result
  }
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    val bin = TrieBranch.findBin(context, offset, k)
    nchildren(bin) = new OptimisticThunk(new PutValueConditionalLazy(nchildren(bin), context, offset + context.branchWidth, k, v, condition))
    result
  }
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = {
    val bin = TrieBranch.findBin(context, offset, k)
    children(bin) = children(bin).get.putInPlace(context, offset + context.branchWidth, k, v)
    this
  }
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    val bin = TrieBranch.findBin(context, offset, k)
    nchildren(bin) = new OptimisticThunk(new UpdateValueLazy(nchildren(bin), context, offset + context.branchWidth, k, f))
    result
  }
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = {
    val bin = TrieBranch.findBin(context, offset, k)
    children(bin) = children(bin).get.updateInPlace(context, offset + context.branchWidth, k, f)
    this
  }
  
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : TrieNode[K,V] = diff.get match {
    case TrieBranch(diffChildren) => {
      val nchildren = children.clone()
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(diffChildren(i) eq TrieEmpty._instance))
          nchildren(i) = new OptimisticThunk(new WriteDiffLazy(nchildren(i), context, offset + context.branchWidth, diffChildren(i)))
      }
      result
    }
    case TrieLeaf(k,v) => put(context, offset, k, v)
    case TrieEmpty() => this
  }
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) : Lazy[TrieNode[K,V]] = diff.get match {
    case TrieBranch(diffChildren) => {
      val nchildren = children.clone()
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(diffChildren(i) eq TrieEmpty._instance))
          nchildren(i) = new OptimisticThunk(new WriteDiffConditionalLazy(nchildren(i), context, offset + context.branchWidth, diffChildren(i), condition))
      }
      result
    }
    case TrieLeaf(k,v) => putConditional(context, offset, k, v, condition)
    case TrieEmpty() => this
  }
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : TrieNode[K,V] = diff.get match {
    case TrieBranch(diffChildren) => {
      val nchildren = children.clone()
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(diffChildren(i) eq TrieEmpty._instance))
          nchildren(i) = new OptimisticThunk(new UpdateDiffLazy(nchildren(i), context, offset + context.branchWidth, diffChildren(i), dcontext))
      }      
      result
    }
    case TrieLeaf(k,f) => update(context, offset, k, f)
    case TrieEmpty() => this
  }
  def map[W](context : Context[K,V], offset : Int, f : (K,V) => Option[W]): TrieNode[K,W] = {
    val nchildren = children.clone().asInstanceOf[Array[Lazy[TrieNode[K,W]]]]
    val result = TrieBranch(nchildren)
    var i = nchildren.length
    while(i > 0) {
      i -= 1
      if(!(nchildren(i) eq TrieEmpty._instance))
        nchildren(i) = new OptimisticThunk(new MapLazy(children(i), context, offset + context.branchWidth, f))
    }
    result
  }
  
  def mergeDD[W,X](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], cx : Context[K,X], offset : Int, left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) : TrieNode[K,X] = other.get match {
    case TrieBranch(otherChildren) => {
      val nchildren = Array.ofDim[Lazy[TrieNode[K,X]]](children.length)
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(otherChildren(i) eq TrieEmpty._instance)) {
          if(!(children(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MergeLazyDD(children(i), otherChildren(i), c1, c2, cx, offset + c1.branchWidth, left, right, merge))
          else if(right.isDefined)
              nchildren(i) = new OptimisticThunk(new MapLazy[K,W,X](otherChildren(i), c2, offset + c1.branchWidth, right.get))
          else
              nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,X]]]
        } else
          if(left.isDefined)
            nchildren(i) = new OptimisticThunk(new MapLazy[K,V,X](children(i), c1, offset + c1.branchWidth, left.get))
          else
            nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,X]]]
      }
      result
    }
    case l@TrieLeaf(k,_) => {
      val nchildren = Array.ofDim[Lazy[TrieNode[K,X]]](children.length)
      val result = TrieBranch(nchildren)
      
      val bin = TrieBranch.findBin(c1, offset, k)
      
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(i != bin)
          if(left.isDefined && !(children(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MapLazy[K,V,X](children(i), c1, offset + c1.branchWidth, left.get))
          else
            nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,X]]]
      }
      
      nchildren(bin) = new OptimisticThunk(new MergeLazyDD(children(bin), l, c1, c2, cx, offset + c1.branchWidth, left, right, merge))
      
      result
    }
    case e@TrieEmpty() => e.mergeDD(this, c2, c1, cx, offset, right, left, (k : K, w : W, v : V) => merge(k,v,w))
  }
  
  def mergeKD[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) : TrieNode[K,V] = other.get match {
    case TrieBranch(otherChildren) => {
      val nchildren = children.clone()
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(otherChildren(i) eq TrieEmpty._instance)) {
          if(!(nchildren(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MergeLazyKD(nchildren(i), otherChildren(i), c1, c2, offset + c1.branchWidth, left, right, merge))
          else if(right.isDefined)
            nchildren(i) = new OptimisticThunk(new MapLazy[K,W,V](otherChildren(i), c2, offset + c1.branchWidth, right.get))
        } else if(left.isDefined && !(nchildren(i) eq TrieEmpty._instance))
          nchildren(i) = new OptimisticThunk(new MapLazy[K,V,V](nchildren(i), c1, offset + c1.branchWidth, left.get))
      }     
      result
    }
    case l@TrieLeaf(k,w) => {
      if(!left.isDefined) {
        update(c1, offset, k, _ match {
          case Some(v) => merge(k,v,w)
          case None => right match {
            case None => None
            case Some(f) => f(k,w)
          }
        })
      } else {
        val nchildren = children.clone()
        val result = TrieBranch(nchildren)
        
        val bin = TrieBranch.findBin(c1, offset, k)
        
        var i = nchildren.length
        while(i > 0) {
          i -= 1
          if(i != bin && !(nchildren(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MapLazy[K,V,V](nchildren(i), c1, offset + c1.branchWidth, left.get))
        }
      
        nchildren(bin) = new OptimisticThunk(new MergeLazyKD(nchildren(bin), l, c1, c2, offset + c1.branchWidth, left, right, merge))
        
        result
      }
    }
    case e@TrieEmpty() => e.mergeDK(this, c2, c1, offset, right, left, (k : K, w : W, v : V) => merge(k,v,w))
  }
  
  def mergeDK[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) : TrieNode[K,W] = other.get match {
    case TrieBranch(otherChildren) => {
      val nchildren = otherChildren.clone()
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(nchildren(i) eq TrieEmpty._instance)) {
          if(!(children(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MergeLazyDK(children(i), nchildren(i), c1, c2, offset + c1.branchWidth, left, right, merge))
          else
            if(right.isDefined)
              nchildren(i) = new OptimisticThunk(new MapLazy[K,W,W](nchildren(i), c2, offset + c1.branchWidth, right.get))
        } else if(left.isDefined && !(children(i) eq TrieEmpty._instance))
          nchildren(i) = new OptimisticThunk(new MapLazy[K,V,W](children(i), c1, offset + c1.branchWidth, left.get))
      }
      result
    }
    case l@TrieLeaf(k,_) => {      
      val nchildren = Array.ofDim[Lazy[TrieNode[K,W]]](children.length)
      val result = TrieBranch(nchildren)
      
      val bin = TrieBranch.findBin(c1, offset, k)
      
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(i != bin && !(children(i) eq TrieEmpty._instance))
          if(left.isDefined)
            nchildren(i) = new OptimisticThunk(new MapLazy[K,V,W](children(i), c1, offset + c1.branchWidth, left.get))
          else
            nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,W]]]
      }
      
      nchildren(bin) = new OptimisticThunk(new MergeLazyDK(children(bin), l, c1, c2, offset + c1.branchWidth, left, right, merge))
      
      result
    }
    case e@TrieEmpty() => e.mergeKD(this, c2, c1, offset, right, left, (k : K, w : W, v : V) => merge(k,v,w))
  }
  
  def mergeKK(other : Lazy[TrieNode[K,V]], c1 : Context[K,V], c2 : Context[K,V], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) : TrieNode[K,V] = other.get match {
    case TrieBranch(otherChildren) => {
      val nchildren = children.clone()
      val result = TrieBranch(nchildren)
      var i = nchildren.length
      while(i > 0) {
        i -= 1
        if(!(otherChildren(i) eq TrieEmpty._instance)) {
          if(!(nchildren(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MergeLazyKK(nchildren(i), otherChildren(i), c1, c2, offset + c1.branchWidth, left, right, merge))
          else if(right.isDefined)
            nchildren(i) = new OptimisticThunk(new MapLazy[K,V,V](otherChildren(i), c2, offset + c1.branchWidth, right.get))
          else
            nchildren(i) = otherChildren(i)
        } else if(left.isDefined && !(nchildren(i) eq TrieEmpty._instance))
          nchildren(i) = new OptimisticThunk(new MapLazy[K,V,V](nchildren(i), c1, offset + c1.branchWidth, left.get))
      }      
      result
    }
    case l@TrieLeaf(k,_) => {
      val nchildren = children.clone()
      val result = TrieBranch(nchildren)
      
      val bin = TrieBranch.findBin(c1, offset, k)
      
      if(left.isDefined) {
        var i = nchildren.length
        while(i > 0) {
          i -= 1
          if(i != bin && !(nchildren(i) eq TrieEmpty._instance))
            nchildren(i) = new OptimisticThunk(new MapLazy[K,V,V](nchildren(i), c1, offset + c1.branchWidth, left.get))
        }
      }
      
      nchildren(bin) = new OptimisticThunk(new MergeLazyKK(nchildren(bin), l, c1, c2, offset + c1.branchWidth, left, right, merge))
      
      result
    }
    case e@TrieEmpty() => e.mergeKK(this, c2, c1, offset, right, left, (k : K, w : V, v : V) => merge(k,v,w))
  }
  
  def removeFrom(context : Context[K,V], offset : Int, low : K, including : Boolean): TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)

    val lbin = TrieBranch.findBin(context, offset, low)
    
    var i = lbin + 1
    while(i < nchildren.length) {
      nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,V]]]
      i += 1
    }
    
    nchildren(lbin) = new OptimisticThunk(new RemoveFromLazy(nchildren(lbin), context, offset + context.branchWidth, low, including))
    
    result
  }
  
  def removeUpto(context : Context[K,V], offset : Int, high : K, including : Boolean): TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)

    val rbin = TrieBranch.findBin(context, offset, high)
    
    var i = 0
    while(i < rbin) {
      nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,V]]]
      i += 1
    }
    
    nchildren(rbin) = new OptimisticThunk(new RemoveUptoLazy(nchildren(rbin), context, offset + context.branchWidth, high, including))
    
    result
  }
  
  def removeRange(context : Context[K,V], offset : Int, low : K, high : K, lowIncluding : Boolean, highIncluding : Boolean): TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)

    val lbin = TrieBranch.findBin(context, offset, low)
    val rbin = TrieBranch.findBin(context, offset, high)
    
    var i = lbin + 1
    while(i < rbin) {
      nchildren(i) = TrieEmpty._instance.asInstanceOf[Lazy[TrieNode[K,V]]]
      i += 1
    }
    
    if(lbin == rbin)
      nchildren(lbin) = new OptimisticThunk(new RemoveRangeLazy(nchildren(lbin), context, offset + context.branchWidth, low, high, lowIncluding, highIncluding)) 
    else {
      nchildren(lbin) = new OptimisticThunk(new RemoveFromLazy(nchildren(lbin), context, offset + context.branchWidth, low, lowIncluding))
      nchildren(rbin) = new OptimisticThunk(new RemoveUptoLazy(nchildren(rbin), context, offset + context.branchWidth, high, highIncluding))
    }
    
    result
  }
  
  def updateRange(context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) : TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)

    val lbin = TrieBranch.findBin(context, offset, low)
    val rbin = TrieBranch.findBin(context, offset, high)
    
    if(lbin == rbin) {
      nchildren(lbin) = new OptimisticThunk(new UpdateRangeLazy(nchildren(lbin), context, offset + context.branchWidth, f, low, high))
    } else {
      nchildren(lbin) = new OptimisticThunk(new UpdateFromLazy(nchildren(lbin), context, offset + context.branchWidth, f, low))
      var i = lbin + 1
      while(i < rbin) {
        nchildren(i) = new OptimisticThunk(new UpdateAllLazy(nchildren(i), context, offset + context.branchWidth, f))
        i += 1
      }
      nchildren(rbin) = new OptimisticThunk(new UpdateUptoLazy(nchildren(rbin), context, offset + context.branchWidth, f, high))
    }
    
    result
  }
  def updateFrom(context : Context[K,V], offset : Int, f : V => Option[V], low : K) = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    val lbin = TrieBranch.findBin(context, offset, low)
    nchildren(lbin) = new OptimisticThunk(new UpdateFromLazy(nchildren(lbin), context, offset + context.branchWidth, f, low)) 
    var i = lbin + 1
    while(i < nchildren.length) {
      nchildren(i) = new OptimisticThunk(new UpdateAllLazy(nchildren(i), context, offset + context.branchWidth, f))
      i += 1
    }
    result
  }
  def updateUpto(context : Context[K,V], offset : Int, f : V => Option[V], high : K) = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    val rbin = TrieBranch.findBin(context, offset, high)
    var i = 0
    while(i < rbin) {
      nchildren(i) = new OptimisticThunk(new UpdateAllLazy(nchildren(i), context, offset + context.branchWidth, f))
      i += 1
    }
    nchildren(rbin) = new OptimisticThunk(new UpdateUptoLazy(nchildren(rbin), context, offset + context.branchWidth, f, high))
    result
  }
  def updateAll(context : Context[K,V], offset : Int, f : V => Option[V]) = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    var i = 0
    while(i < nchildren.length) {
      nchildren(i) = new OptimisticThunk(new UpdateAllLazy(nchildren(i), context, offset + context.branchWidth, f))
      i += 1
    }
    result
  }
  
  def replaceRange(context : Context[K,V], offset : Int, f : V => V, low : K, high : K) : TrieNode[K,V] = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)

    val lbin = TrieBranch.findBin(context, offset, low)
    val rbin = TrieBranch.findBin(context, offset, high)
    
    if(lbin == rbin) {
      nchildren(lbin) = new OptimisticThunk(new ReplaceRangeLazy(nchildren(lbin), context, offset + context.branchWidth, f, low, high))
    } else {
      nchildren(lbin) = new OptimisticThunk(new ReplaceFromLazy(nchildren(lbin), context, offset + context.branchWidth, f, low))
      var i = lbin + 1
      while(i < rbin) {
        nchildren(i) = new OptimisticThunk(new ReplaceAllLazy(nchildren(i), context, offset + context.branchWidth, f))
        i += 1
      }
      nchildren(rbin) = new OptimisticThunk(new ReplaceUptoLazy(nchildren(rbin), context, offset + context.branchWidth, f, high))
    }
    
    result
  }
  def replaceFrom(context : Context[K,V], offset : Int, f : V => V, low : K) = {
    val lbin = TrieBranch.findBin(context, offset, low)
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    nchildren(lbin) = new OptimisticThunk(new ReplaceFromLazy(nchildren(lbin), context, offset + context.branchWidth, f, low)) 
    var i = lbin + 1
    while(i < nchildren.length) {
      nchildren(i) = new OptimisticThunk(new ReplaceAllLazy(nchildren(i), context, offset + context.branchWidth, f))
      i += 1
    }
    result
  }
  def replaceUpto(context : Context[K,V], offset : Int, f : V => V, high : K) = {
    val rbin = TrieBranch.findBin(context, offset, high)
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    var i = 0
    while(i < rbin) {
      nchildren(i) = new OptimisticThunk(new ReplaceAllLazy(nchildren(i), context, offset + context.branchWidth, f))
      i += 1
    }
    nchildren(rbin) = new OptimisticThunk(new ReplaceUptoLazy(nchildren(rbin), context, offset + context.branchWidth, f, high))
    result
  }
  def replaceAll(context : Context[K,V], offset : Int, f : V => V) = {
    val nchildren = children.clone()
    val result = TrieBranch(nchildren)
    var i = 0
    while(i < nchildren.length) {
      nchildren(i) = new OptimisticThunk(new ReplaceAllLazy(nchildren(i), context, offset + context.branchWidth, f))
      i += 1
    }
    result
  }
  
  def reduceRange[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K, high : K) : T = {
    val lbin = TrieBranch.findBin(context, offset, low)
    val rbin = TrieBranch.findBin(context, offset, high)
    if(lbin == rbin) {
      children(lbin).get.reduceRange(context, offset + context.branchWidth, accum, f, low, high)
    } else {
      var a = children(lbin).get.reduceFrom(context, offset + context.branchWidth, accum, f, low)
      var i = lbin + 1
      while(i < rbin) {
        a = children(i).get.reduceAll(a, f)
        i += 1
      }
      children(rbin).get.reduceUpto(context, offset + context.branchWidth, a, f, high)
    }
  }
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T = {
    val lbin = TrieBranch.findBin(context, offset, low)
    var a = children(lbin).get.reduceFrom(context, offset + context.branchWidth, accum, f, low)
    var i = lbin + 1
    while(i < children.length) {
      a = children(i).get.reduceAll(a, f)
      i += 1
    }
    a
  }
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T = {
    val rbin = TrieBranch.findBin(context, offset, high)
    var a = accum
    var i = 0
    while(i < rbin) {
      a = children(i).get.reduceAll(a, f)
      i += 1
    }
    children(rbin).get.reduceUpto(context, offset + context.branchWidth, a, f, high)
  }
  def reduceAll[T](accum : T, f : (T,V) => T) : T = {
    var a = accum
    var i = children.length
    while(i > 0) {
      i -= 1
      a = children(i).get.reduceAll(a, f)
    }
    a
  }
  
  def compact(context : Context[K,V]) = {
    var allEmpty = true
    var i = 0
    
    while(i < children.length) {
      children(i) = getChild(i).compact(context)
      if(!(children(i) eq TrieEmpty._instance))
        allEmpty = false
      i += 1
    }
    
    if(allEmpty)
      TrieEmpty.instance
    else
      this
  }
  
  def force(context : Context[K,V], offset : Int, k : K) : Unit = {
    val bin = TrieBranch.findBin(context, offset, k)
    getChild(bin).force(context, offset + context.branchWidth, k)
  }
  def forceRange(context : Context[K,V], offset : Int, low : K, high : K) : Unit = {
    val lbin = TrieBranch.findBin(context, offset, low)
    val rbin = TrieBranch.findBin(context, offset, high)
    if(lbin == rbin) {
      getChild(lbin).forceRange(context, offset + context.branchWidth, low, high)
    } else {
      getChild(lbin).forceFrom(context, offset + context.branchWidth, low)
      
      var i = lbin + 1
      while(i < rbin) {
        getChild(i).forceAll()
        i += 1
      }
      
      getChild(rbin).forceUpto(context, offset + context.branchWidth, high)
    }
  }
  def forceFrom(context : Context[K,V], offset : Int, low : K) : Unit = {
    val lbin = TrieBranch.findBin(context, offset, low)
    getChild(lbin).forceFrom(context, offset + context.branchWidth, low)
    var i = lbin + 1
    while(i < children.length) {
      getChild(i).forceAll()
      i += 1
    }
  }
  def forceUpto(context : Context[K,V], offset : Int, high : K) : Unit = {
    val rbin = TrieBranch.findBin(context, offset, high)
    var i = 0
    while(i < rbin) {
      getChild(i).forceAll()
      i += 1
    }
    getChild(rbin).forceUpto(context, offset + context.branchWidth, high)
  }
  def forceAll() : Unit = {
    var i = children.length
    while(i > 0) {
      i -= 1
      getChild(i).forceAll()
    }
  }
  def forceKeys(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = keys match {
    case TrieBranch(otherChildren) => {
      // Random
      /*
      val pivot = ThreadLocalRandom.current.nextInt(children.length)
      
      var i = pivot
      while(i < children.length) {
        if(!(otherChildren(i) eq TrieEmpty._instance) && !(children(i) eq TrieEmpty._instance)) {
          getChild(i).forceKeys(context, offset + context.branchWidth, otherChildren(i).get)
        }
        i += 1
      }
      
      i = 0
      while(i < pivot) {
        if(!(otherChildren(i) eq TrieEmpty._instance) && !(children(i) eq TrieEmpty._instance)) {
          getChild(i).forceKeys(context, offset + context.branchWidth, otherChildren(i).get)
        }
        i += 1
      }
      */
      
      var i = 0
      while(i < children.length) {
        if(!(otherChildren(i) eq TrieEmpty._instance) && !(children(i) eq TrieEmpty._instance)) {
          getChild(i).forceKeys(context, offset + context.branchWidth, otherChildren(i).get)
        }
        i += 1
      }
      
      /*
      var i = children.length
      while(i > 0) {
        i -= 1
        if(!(otherChildren(i) eq TrieEmpty._instance) && !(children(i) eq TrieEmpty._instance)) {
          getChild(i).forceKeys(context, offset + context.branchWidth, otherChildren(i).get)
        }
      }
      */
    }
    case TrieLeaf(k,_) => {
      force(context, offset, k)
    }
    case TrieEmpty() => {}
  }
  def forceBranches(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = keys match {
    case TrieBranch(otherChildren) => {
      var i = children.length
      while(i > 0) {
        i -= 1
        if(!(otherChildren(i) eq TrieEmpty._instance) && !(children(i) eq TrieEmpty._instance)) {
          children(i).get.forceKeys(context, offset + context.branchWidth, otherChildren(i).get)
        }
      }
    }
    case TrieLeaf(k,_) => {}
    case TrieEmpty() => {}
  }
  
  def printDebug = "[" + children.map(_.get.printDebug).mkString(",") + "]"
}

abstract class TrieFunction[K,V] extends TrieNode[K,V] {  
  def isEvaluated = false
  def get = evaluate.get
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = throw new Exception
  def floor(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = throw new Exception
  def ceil(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = throw new Exception
  def prev(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = throw new Exception
  def next(context : Context[K,V], offset : Int, k : K) : Option[(K,V)] = throw new Exception
  def first : Option[(K,V)] = throw new Exception
  def last : Option[(K,V)] = throw new Exception
  def foreach(f : (K,V) => Unit) : Unit = throw new Exception
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = throw new Exception
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V] = throw new Exception
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = throw new Exception
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = throw new Exception
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = throw new Exception
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : TrieNode[K,V] = throw new Exception
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) : Lazy[TrieNode[K,V]] = throw new Exception
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : TrieNode[K,V] = throw new Exception
  def map[W](context : Context[K,V], offset : Int, f : (K,V) => Option[W]): TrieNode[K,W] = throw new Exception
  def mergeDD[W,X](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], cx : Context[K,X], offset : Int, left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) : TrieNode[K,X] = throw new Exception
  def mergeKD[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) : TrieNode[K,V] = throw new Exception
  def mergeDK[W](other : Lazy[TrieNode[K,W]], c1 : Context[K,V], c2 : Context[K,W], offset : Int, left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) : TrieNode[K,W] = throw new Exception
  def mergeKK(other : Lazy[TrieNode[K,V]], c1 : Context[K,V], c2 : Context[K,V], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) : TrieNode[K,V] = throw new Exception
  def removeFrom(context : Context[K,V], offset : Int, low : K, including : Boolean): TrieNode[K,V] = throw new Exception
  def removeUpto(context : Context[K,V], offset : Int, high : K, including : Boolean): TrieNode[K,V] = throw new Exception
  def removeRange(context : Context[K,V], offset : Int, low : K, high : K, lowIncluding : Boolean, highIncluding : Boolean): TrieNode[K,V] = throw new Exception
  def updateRange(context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) = throw new Exception
  def updateFrom(context : Context[K,V], offset : Int, f : V => Option[V], low : K) = throw new Exception
  def updateUpto(context : Context[K,V], offset : Int, f : V => Option[V], high : K) = throw new Exception
  def updateAll(context : Context[K,V], offset : Int, f : V => Option[V]) = throw new Exception
  def replaceRange(context : Context[K,V], offset : Int, f : V => V, low : K, high : K) : TrieNode[K,V] = throw new Exception
  def replaceFrom(context : Context[K,V], offset : Int, f : V => V, low : K) : TrieNode[K,V] = throw new Exception
  def replaceUpto(context : Context[K,V], offset : Int, f : V => V, high : K) : TrieNode[K,V] = throw new Exception
  def replaceAll(context : Context[K,V], offset : Int, f : V => V) : TrieNode[K,V] = throw new Exception
  def reduceRange[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K, high : K) : T = throw new Exception
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T = throw new Exception
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T = throw new Exception
  def reduceAll[T](accum : T, f : (T,V) => T) : T = throw new Exception
  def compact(context : Context[K,V]) = throw new Exception
  def force(context : Context[K,V], offset : Int, k : K) : Unit = throw new Exception
  def force[W](context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,W]]) : Unit = throw new Exception
  def forceRange(context : Context[K,V], offset : Int, low : K, high : K) : Unit = throw new Exception
  def forceFrom(context : Context[K,V], offset : Int, low : K) : Unit = throw new Exception
  def forceUpto(context : Context[K,V], offset : Int, high : K) : Unit = throw new Exception
  def forceAll() : Unit = throw new Exception
  def forceKeys(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = throw new Exception
  def forceBranches(context : Context[K,V], offset : Int, keys : TrieNode[K,_]) = throw new Exception
  def printDebug = throw new Exception
}

class UpdateValueLazy[K,V](var trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) extends TrieFunction[K,V] {
  def evaluate = trie.get.update(context, offset, k, f)
}

class PutValueLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, k : K, v : V) extends TrieFunction[K,V] {
  def evaluate = trie.get.put(context, offset, k, v)
}

class PutValueConditionalLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) extends TrieFunction[K,V] {
  def evaluate = {
    if(condition.isEvaluated) {
      if(condition.get) {
        trie.get.put(context, offset, k, v)
      } else {
        trie
      }
    } else {
      trie.get.putConditional(context, offset, k, v, condition)
    }
  }
}

class UpdateDiffLazy[K,V](var trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) extends TrieFunction[K,V] {
  def evaluate = trie.get.update(context, offset, diff, dcontext)
}

class WriteDiffLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) extends TrieFunction[K,V] {
  def evaluate = trie.get.write(context, offset, diff)
}

class WriteDiffConditionalLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) extends TrieFunction[K,V] {
  def evaluate = {
    if(condition.isEvaluated) {
      if(condition.get) {
        trie.get.write(context, offset, diff)
      } else {
        trie
      }
    } else {
      trie.get.writeConditional(context, offset, diff, condition)
    }
  }
}

class MapLazy[K,V,W](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : (K,V) => Option[W]) extends TrieFunction[K,W] {
  def evaluate = trie.get.map(context, offset, f)
}

class MergeLazyDD[K,V,W,X](a : Lazy[TrieNode[K,V]], b : Lazy[TrieNode[K,W]], ca : Context[K,V], cb : Context[K,W], cx : Context[K,X], offset : Int, left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) extends TrieFunction[K,X] {
  def evaluate = a.get.mergeDD(b, ca, cb, cx, offset, left, right, merge)
}

class MergeLazyKD[K,V,W](a : Lazy[TrieNode[K,V]], b : Lazy[TrieNode[K,W]], ca : Context[K,V], cb : Context[K,W], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) extends TrieFunction[K,V] {
  def evaluate = a.get.mergeKD(b, ca, cb, offset, left, right, merge)
}

class MergeLazyDK[K,V,W](a : Lazy[TrieNode[K,V]], b : Lazy[TrieNode[K,W]], ca : Context[K,V], cb : Context[K,W], offset : Int, left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) extends TrieFunction[K,W] {
  def evaluate = a.get.mergeDK(b, ca, cb, offset, left, right, merge)
}

class MergeLazyKK[K,V](a : Lazy[TrieNode[K,V]], b : Lazy[TrieNode[K,V]], ca : Context[K,V], cb : Context[K,V], offset : Int, left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) extends TrieFunction[K,V] {
  def evaluate = a.get.mergeKK(b, ca, cb, offset, left, right, merge)
}

class RemoveFromLazy[K, V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, low : K, including : Boolean) extends TrieFunction[K,V] {
  def evaluate = trie.get.removeFrom(context, offset, low, including)
}

class RemoveUptoLazy[K, V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, high : K, including : Boolean) extends TrieFunction[K,V] {
  def evaluate = trie.get.removeUpto(context, offset, high, including)
}

class RemoveRangeLazy[K, V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, low : K, high : K, lowIncluding : Boolean, highIncluding : Boolean) extends TrieFunction[K,V] {
  def evaluate = trie.get.removeRange(context, offset, low, high, lowIncluding, highIncluding)
}

class UpdateRangeLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) extends TrieFunction[K,V] {
  def evaluate = trie.get.updateRange(context, offset, f, low, high)
}

class UpdateFromLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => Option[V], low : K) extends TrieFunction[K,V] {
  def evaluate = trie.get.updateFrom(context, offset, f, low)
}

class UpdateUptoLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => Option[V], high : K) extends TrieFunction[K,V] {
  def evaluate = trie.get.updateUpto(context, offset, f, high)
}

class UpdateAllLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => Option[V]) extends TrieFunction[K,V] {
  def evaluate = trie.get.updateAll(context, offset, f)
}

class ReplaceRangeLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => V, low : K, high : K) extends TrieFunction[K,V] {
  def evaluate = trie.get.replaceRange(context, offset, f, low, high)
}

class ReplaceFromLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => V, low : K) extends TrieFunction[K,V] {
  def evaluate = trie.get.replaceFrom(context, offset, f, low)
}

class ReplaceUptoLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => V, high : K) extends TrieFunction[K,V] {
  def evaluate = trie.get.replaceUpto(context, offset, f, high)
}

class ReplaceAllLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : V => V) extends TrieFunction[K,V] {
  def evaluate = trie.get.replaceAll(context, offset, f)
}

class ShiftLazy[V](trie : Lazy[TrieNode[Int,V]], context : Context[Int,V], offset : Int, low : Int, shift : Int) extends TrieFunction[Int,V] {
  def evaluate = {
    val t = trie.get
    
    val high = low + (1 << (context.key.maxBits - offset))
    
    // Note, this is a bit of a hack, we should make next, prev etc work correctly for values out of range
    var ql = low - shift
    if(ql < 0) ql = 0
    if(ql >= (1 << context.key.maxBits)) {
      TrieEmpty._instance.asInstanceOf[TrieNode[Int,V]] 
    } else {
      t.ceil(context, context.offset, ql) match {
        case Some((k,v)) if k < high - shift => t.next(context, context.offset, k) match {
          case Some((l,_)) if l < high - shift =>
            val binSize = 1 << (context.key.maxBits - offset - context.branchWidth)

            val b = context.emptyBranch.copy
            var i = b.children.length
            while(i > 0) {
              i -= 1                  
              b.children(i) = new OptimisticThunk(new ShiftLazy(t, context, offset + context.branchWidth, low + binSize * i, shift))
            }
            
            b
          case _ => TrieLeaf(k + shift, v)
        }
        case _ => TrieEmpty._instance.asInstanceOf[TrieNode[Int,V]] 
      }
    }
  }
}

//class MapFillLazy
	// This is equivalent to generating a range of values in a new map, and merging that new map with the current one