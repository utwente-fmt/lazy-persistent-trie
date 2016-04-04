package lazytrie

import java.util.LinkedList
import scala.reflect.ClassTag
import laziness.OptimisticThunk
import laziness.Lazy
import java.util.Arrays

case class Context[K,V](
  leafWidth : Int,
  branchWidth : Int,
  key : Key[K],
  emptyBranch : TrieBranch[K,V],
  ktag : ClassTag[K],
  vtag : ClassTag[V],
  offset : Int
)

abstract class TrieNode[K,V] extends Lazy[TrieNode[K,V]] {
  def get(context : Context[K,V], offset : Int, k : K) : Option[V]
  
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V]
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V]
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V]
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V]
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V]
  
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : Lazy[TrieNode[K,V]]
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) : Lazy[TrieNode[K,V]]
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : Lazy[TrieNode[K,V]]
  
  def map[W:ClassTag](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) : TrieNode[K,W]
  
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
    diff.get.map(dcontext, offset, (k : K, f : Option[V] => Option[V]) => f(None))(context.vtag)
    
  def map[W:ClassTag](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) = TrieEmpty.instance

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
  
  def printDebug = "E"
}

case class TrieLeaf[K,V](key : K, var value : V) extends TrieNode[K,V] {
  private def split(context : Context[K,V], size : Int) : TrieNode[K,V] =
    if(context.leafWidth != 0)
      WideLeaf(context.ktag.newArray(size), context.vtag.newArray(size), 0)
    else
      context.emptyBranch.copy
  
  def get = this
  def evaluate = this
  def isEvaluated = true
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = if(context.key.eq(k, key)) Some(value) else None
  
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] =
    if(context.key.eq(k, key)) {
      TrieLeaf(key, v)
    } else {
      val r = split(context, 2)
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
      val r = split(context, 1 << context.leafWidth)
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
      val r = split(context, 2)
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
      val r = split(context, 1 << context.leafWidth)
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
      }).map(dcontext, offset, (k, f) => f(None))(context.vtag)
  }
  
  def map[W:ClassTag](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) = f(key, value) match {
    case Some(v) => TrieLeaf(key, v)
    case None => TrieEmpty.instance
  }
  
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
  
  def printDebug = key + " -> " + value
}

case class WideLeaf[K,V](keys : Array[K], values : Array[V], var end : Int) extends TrieNode[K,V] {
  def this(context : Context[K,V], size : Int) = this(context.ktag.newArray(size), context.vtag.newArray(size), 0)
  
  def get = this
  def evaluate = this
  def isEvaluated = true

  private def indexOf(context : Context[K,V], k : K) : Int = {
    val d = context.key
    var i = end
    while(i > 0) {
      i -= 1
      if(d.eq(k, keys(i)))
        return i
    }
    -1
  }
  
  private def split(context : Context[K,V], offset : Int) = {
    var r = context.emptyBranch.copy
    var i = end
    while(i > 0) {
      i -= 1
      r.putInPlace(context, offset, keys(i), values(i))
    }
    r
  }
  
  private def append(context : Context[K,V], offset : Int, k : K, v : V) = {
    if(end == 1 << context.leafWidth) {
      split(context, offset).put(context, offset, k, v)
    } else {
      // Note, we could share arrays with this version, provided that the data structure is used in a linear way
      val nkeys = context.ktag.newArray(end + 1)
      val nvalues = context.vtag.newArray(end + 1)
      System.arraycopy(keys, 0, nkeys, 0, end)
      System.arraycopy(values, 0, nvalues, 0, end)
      nkeys(end) = k
      nvalues(end) = v
      WideLeaf(nkeys, nvalues, end + 1)
    }
  }
  
  private def appendInPlace(context : Context[K,V], offset : Int, k : K, v : V) = {
    if(end == 1 << context.leafWidth) {
      split(context, offset).putInPlace(context, offset, k, v)
    } else {
      if(keys.length > end) {
        keys(end) = k
        values(end) = v
        end += 1
        this
      } else {
        val nkeys = context.ktag.newArray(1 << context.leafWidth)
        val nvalues = context.vtag.newArray(1 << context.leafWidth)
        System.arraycopy(keys, 0, nkeys, 0, end)
        System.arraycopy(values, 0, nvalues, 0, end)
        nkeys(end) = k
        nvalues(end) = v
        WideLeaf(nkeys, nvalues, end + 1)
      }
    }
  }
  
  private def remove(context : Context[K,V], index : Int) : TrieNode[K,V] = {
    if(end == 1) {
      TrieEmpty.instance
    } else {
      val nkeys = context.ktag.newArray(end - 1)
      val nvalues = context.vtag.newArray(end - 1)
      System.arraycopy(keys, 0, nkeys, 0, index)
      System.arraycopy(values, 0, nvalues, 0, index)
      if(index + 1 < end) {
        System.arraycopy(keys, index + 1, nkeys, index, end - index - 1)
        System.arraycopy(values, index + 1, nvalues, index, end - index - 1)
      }
      WideLeaf(nkeys, nvalues, end - 1)
    }
  }
  
  private def replaceValue(index : Int, v : V) = {
    val r = WideLeaf(keys, values.clone(), end)
    r.values(index) = v
    r
  }
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = {
    val i = indexOf(context, k)
    if(i >= 0)
      Some(values(i))
    else
      None
  }
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = {
    val i = indexOf(context, k)
    if(i >= 0)
      replaceValue(i, v)
    else     
      append(context, offset, k, v)
  }
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V] =
    if(condition.get) put(context, offset, k, v) else this
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = {
    val i = indexOf(context, k)
    if(i >= 0) {
      values(i) = v
      this
    } else
      appendInPlace(context, offset, k, v)
  }
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) = {
    val i = indexOf(context, k)
    if(i >= 0) {
      f(Some(values(i))) match {
        case Some(v) => replaceValue(i, v)
        case None => remove(context, i)
      }
    } else {
      f(None) match {
        case Some(v) => append(context, offset, k, v)
        case None => this
      }
    }
  }
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = {
    val i = indexOf(context, k)
    if(i >= 0) {
      f(Some(values(i))) match {
        case Some(v) => values(i) = v; this
        case None => remove(context, i)
      }
    } else {
      f(None) match {
        case Some(v) => appendInPlace(context, offset, k, v)
        case None => this
      }
    }
  }
  
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : TrieNode[K,V] = diff.get match {
      case WideLeaf(ks,vs,e) =>
        var r : TrieNode[K,V] = WideLeaf(keys, values, end)    // Note that this either requires the keys and values array to have no remaining space, or we must update linearly, otherwise this fails
        var i = e
        while(i > 0) {
          i -= 1
          r = r.putInPlace(context, offset, ks(i), vs(i))
        }
        r
      case TrieBranch(cs) =>
        val r = TrieBranch(cs.clone)
        var i = end
        while(i > 0) {
          i -= 1
          val x = values(i)
          val bin = TrieBranch.findBin(context, offset, keys(i))
          r.children(bin) = r.children(bin).get.update(context, offset + context.branchWidth, keys(i), (v : Option[V]) => v match {
            case None => Some(x)
            case sv => sv
          })
        }
        r
      case l@TrieLeaf(k,v) =>
        put(context, offset, k, v)
      case TrieEmpty() => this
    }
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) =
    if(condition.get) write(context, offset, diff) else this
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : TrieNode[K,V] = diff.get match {
    case WideLeaf(ks,vs,e) =>
      var r : TrieNode[K,V] = this
      var i = e
      while(i > 0) {
        i -= 1
        r = r.update(context, offset, ks(i), vs(i)) 
      }
      r
    case b@TrieBranch(cs) =>
      val r = b.copy
      var i = end
      while(i > 0) {
        i -= 1
        val v = values(i)
        val bin = TrieBranch.findBin(context, offset, keys(i))
        r.children(bin) = r.children(bin).get.update(dcontext, offset + context.branchWidth, keys(i),  _ match {
          case None => Some(_ => Some(v))
          case Some(f) => Some(_ => f(Some(v)))
        })
      }
      r.map(dcontext, offset, (k,f) => f(None))(context.vtag)
    case TrieLeaf(k,v) => update(context, offset, k, v)
    case TrieEmpty() => this
  }
  
  def map[W:ClassTag](context : Context[K,V], offset : Int, f : (K,V) => Option[W]) = {
    val nkeys = context.ktag.newArray(end)
    val nvalues = Array.ofDim[W](end)
    var c = 0
    var i = end
    while(i > 0) {
      i -= 1
      f(keys(i), values(i)) match {
        case Some(v) =>
          nkeys(c) = keys(i)
          nvalues(c) = v
          c += 1
        case None => {}
      }
    }
    WideLeaf(nkeys, nvalues, c)
  }
  
  def updateRange(context : Context[K,V], offset : Int, f : V => Option[V], low : K, high : K) = {
    val nkeys = context.ktag.newArray(end)
    val nvalues = context.vtag.newArray(end)
    var c = 0
    var i = end
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), high) <= 0 && context.key.compare(keys(i), low) >= 0) {
        f(values(i)) match {
          case Some(v) =>
            nkeys(c) = keys(i)
            nvalues(c) = v
            c += 1
          case None => {}
        }
      } else {
        nkeys(c) = keys(i)
        nvalues(c) = values(i)
        c += 1
      }
    }
    WideLeaf(nkeys, nvalues, c)
  }
  def updateFrom(context : Context[K,V], offset : Int, f : V => Option[V], low : K) = {
    val nkeys = context.ktag.newArray(end)
    val nvalues = context.vtag.newArray(end)
    var c = 0
    var i = end
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), low) >= 0) {
        f(values(i)) match {
          case Some(v) =>
            nkeys(c) = keys(i)
            nvalues(c) = v
            c += 1
          case None => {}
        }
      } else {
        nkeys(c) = keys(i)
        nvalues(c) = values(i)
        c += 1
      }
    }
    WideLeaf(nkeys, nvalues, c)
  }
  def updateUpto(context : Context[K,V], offset : Int, f : V => Option[V], high : K) = {
    val nkeys = context.ktag.newArray(end)
    val nvalues = context.vtag.newArray(end)
    var c = 0
    var i = end
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), high) <= 0) {
        f(values(i)) match {
          case Some(v) =>
            nkeys(c) = keys(i)
            nvalues(c) = v
            c += 1
          case None => {}
        }
      } else {
        nkeys(c) = keys(i)
        nvalues(c) = values(i)
        c += 1
      }
    }
    WideLeaf(nkeys, nvalues, c)
  }
  def updateAll(context : Context[K,V], offset : Int, f : V => Option[V]) = {
    val nkeys = context.ktag.newArray(end)
    val nvalues = context.vtag.newArray(end)
    var c = 0
    var i = end
    while(i > 0) {
      i -= 1
      f(values(i)) match {
        case Some(v) =>
          nkeys(c) = keys(i)
          nvalues(c) = v
          c += 1
        case None => {}
      }
    }
    WideLeaf(nkeys, nvalues, c)
  }
  
  def replaceRange(context : Context[K,V], offset : Int, f : V => V, low : K, high : K) = {
    val nvalues = context.vtag.newArray(end)
    var i = end
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), high) <= 0 && context.key.compare(keys(i), low) >= 0) {
        nvalues(i) = f(values(i))
      } else {
        nvalues(i) = values(i)
      }
    }
    WideLeaf(keys, nvalues, end)
  }
  def replaceFrom(context : Context[K,V], offset : Int, f : V => V, low : K) = {
    val nvalues = context.vtag.newArray(end)
    var i = end
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), low) >= 0) {
        nvalues(i) = f(values(i))
      } else {
        nvalues(i) = values(i)
      }
    }
    WideLeaf(keys, nvalues, end)
  }
  def replaceUpto(context : Context[K,V], offset : Int, f : V => V, high : K) = {
    val nvalues = context.vtag.newArray(end)
    var i = end
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), high) <= 0) {
        nvalues(i) = f(values(i))
      } else {
        nvalues(i) = values(i)
      }
    }
    WideLeaf(keys, nvalues, end)
  }
  def replaceAll(context : Context[K,V], offset : Int, f : V => V) = {
    val nvalues = context.vtag.newArray(end)
    var c = 0
    var i = end
    while(i > 0) {
      i -= 1
      nvalues(i) = f(values(i))
    }
    WideLeaf(keys, nvalues, end)
  }
  
  
  def reduceRange[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K, high : K) : T = {
    var a = accum
    var i = values.length
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), high) <= 0 && context.key.compare(keys(i), low) >= 0)
        a = f(a, values(i))
    }
    a
  }
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T ={
    var a = accum
    var i = values.length
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), low) >= 0)
        a = f(a, values(i))
    }
    a
  }
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T = {
    var a = accum
    var i = values.length
    while(i > 0) {
      i -= 1
      if(context.key.compare(keys(i), high) <= 0)
        a = f(a, values(i))
    }
    a
  }
  def reduceAll[T](accum : T, f : (T,V) => T) : T = {
    var a = accum
    var i = values.length
    while(i > 0) {
      i -= 1
      a = f(a, values(i))
    }
    a
  }

  def compact(context : Context[K,V]) = this
  
  def force(context : Context[K,V], offset : Int, k : K) : Unit = {}
  def forceRange(context : Context[K,V], offset : Int, low : K, high : K) : Unit = {}
  def forceFrom(context : Context[K,V], offset : Int, low : K) : Unit = {}
  def forceUpto(context : Context[K,V], offset : Int, high : K) : Unit = {}
  def forceAll() : Unit = {}
  
  def printDebug = keys + " -> " + values
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
    if(!(c eq ec))
      children(bin) = ec
    ec
  }
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = {
    val bin = TrieBranch.findBin(context, offset, k)
    getChild(bin).get(context, offset + context.branchWidth, k)
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
    case WideLeaf(ks,vs,e) => {
      var i = e
      var r = copy
      while(i > 0) {
        i -= 1
        val bin = TrieBranch.findBin(context, offset, ks(i))
        r.children(bin) = new OptimisticThunk(new PutValueLazy(r.children(bin), context, offset + context.branchWidth, ks(i), vs(i)))
      }
      r
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
    case WideLeaf(ks,vs,e) => {
      var i = e
      var r = copy
      while(i > 0) {
        i -= 1
        val bin = TrieBranch.findBin(context, offset, ks(i))
        r.children(bin) = new OptimisticThunk(new PutValueConditionalLazy(r.children(bin), context, offset + context.branchWidth, ks(i), vs(i), condition))
      }
      r
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
    case WideLeaf(ks,vs,e) => {
      var i = 0
      var r = copy
      while(i < e) {
        val bin = TrieBranch.findBin(context, offset, ks(i))       
        r.children(bin) = new OptimisticThunk(new UpdateValueLazy(r.children(bin), context, offset + context.branchWidth, ks(i), vs(i)))
        i += 1
      }
      r
    }
    case TrieLeaf(k,f) => update(context, offset, k, f)
    case TrieEmpty() => this
  }
  def map[W:ClassTag](context : Context[K,V], offset : Int, f : (K,V) => Option[W]): TrieNode[K,W] = {
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
      getChild(lbin).reduceRange(context, offset + context.branchWidth, accum, f, low, high)
    } else {
      var a = getChild(lbin).reduceFrom(context, offset + context.branchWidth, accum, f, low)
      var i = lbin + 1
      while(i < rbin) {
        a = getChild(i).reduceAll(a, f)
        i += 1
      }
      getChild(rbin).reduceUpto(context, offset + context.branchWidth, a, f, high)
    }
  }
  def reduceFrom[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, low : K) : T = {
    val lbin = TrieBranch.findBin(context, offset, low)
    var a = getChild(lbin).reduceFrom(context, offset + context.branchWidth, accum, f, low)
    var i = lbin + 1
    while(i < children.length) {
      a = getChild(i).reduceAll(a, f)
      i += 1
    }
    a
  }
  def reduceUpto[T](context : Context[K,V], offset : Int, accum : T, f : (T,V) => T, high : K) : T = {
    val rbin = TrieBranch.findBin(context, offset, high)
    var a = accum
    var i = 0
    while(i < rbin) {
      a = getChild(i).reduceAll(a, f)
      i += 1
    }
    getChild(rbin).reduceUpto(context, offset + context.branchWidth, a, f, high)
  }
  def reduceAll[T](accum : T, f : (T,V) => T) : T = {
    var a = accum
    var i = children.length
    while(i > 0) {
      i -= 1
      a = getChild(i).reduceAll(a, f)
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
  
  def printDebug = "[" + children.map(_.get.printDebug).mkString(",") + "]"
}

abstract class TrieFunction[K,V] extends TrieNode[K,V] {  
  def isEvaluated = false
  def get = evaluate.get
  
  def get(context : Context[K,V], offset : Int, k : K) : Option[V] = throw new Exception
  def put(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = throw new Exception
  def putConditional(context : Context[K,V], offset : Int, k : K, v : V, condition : Lazy[Boolean]) : TrieNode[K,V] = throw new Exception
  def putInPlace(context : Context[K,V], offset : Int, k : K, v : V) : TrieNode[K,V] = throw new Exception
  def update(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = throw new Exception
  def updateInPlace(context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) : TrieNode[K,V] = throw new Exception
  def write(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]]) : TrieNode[K,V] = throw new Exception
  def writeConditional(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K,V]], condition : Lazy[Boolean]) : Lazy[TrieNode[K,V]] = throw new Exception
  def update(context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) : TrieNode[K,V] = throw new Exception
  def map[W:ClassTag](context : Context[K,V], offset : Int, f : (K,V) => Option[W]): TrieNode[K,W] = throw new Exception
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
  def printDebug = throw new Exception
}

class UpdateValueLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, k : K, f : Option[V] => Option[V]) extends TrieFunction[K,V] {
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

class UpdateDiffLazy[K,V](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, diff : Lazy[TrieNode[K, Option[V] => Option[V]]], dcontext : Context[K,Option[V] => Option[V]]) extends TrieFunction[K,V] {
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

class MapLazy[K,V,W:ClassTag](trie : Lazy[TrieNode[K,V]], context : Context[K,V], offset : Int, f : (K,V) => Option[W]) extends TrieFunction[K,W] {
  def evaluate = trie.get.map(context, offset, f)
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