package lazytrie

import java.util.LinkedList
import laziness.OptimisticThunk
import laziness.Lazy
import scala.collection.mutable.ArrayBuffer
import lazytx.Diff
import lazytx.MapDiff

object Map {  
  def apply[K,V](branchWidth : Int = 5)(implicit key : Key[K]) : Map[K,V] = {
    val emptyBranch = new TrieBranch[K,V](Array.ofDim[TrieNode[K,V]](1 << branchWidth).map(_ => TrieEmpty.instance[K,V]))
    
    // Compute how many zero bits need to be padded at the root to align the leafs
    // E.g. if we have 21 bits, and width 4, the leaves would store only 1 bit, which wastes a lot of memory
    // To solve this, we need to pad 3 bits at the root, and we compute the initial offset as -3
    val branchBits = key.maxBits
    var offset = 0
    val roundedDown = ((branchBits / branchWidth) * branchWidth)
    if(key.maxBits >= 0 && roundedDown != branchBits)
      offset = (branchBits - roundedDown) - branchWidth

    new Map(Context(branchWidth, key, emptyBranch, offset), TrieEmpty.instance)
  }
  
  def emptyInt[V](capacity : Int, branchWidth : Int = 5) =
    Map[Int,V](branchWidth)(SmallIntKey.make(capacity))
    
  def update[K,V](context : Context[K,V], diff : Map[K, Option[V] => Option[V]]) : Map[K,V] => Map[K,V] = {
    val udl = new UpdateDiffLazy(null, context, context.offset, diff.root, diff.context)
    val result = new Map(context, new OptimisticThunk(udl))
    m => {
      udl.trie = m.root
      result
    }
  }
  
  def replace[K,V](context : Context[K,V], k : K, f : Option[V] => Option[V]) : Map[K,V] => Map[K,V] = {
    val udl = new UpdateValueLazy(null, context, context.offset, k, f)
    val result = new Map(context, new OptimisticThunk(udl))
    m => {
      udl.trie = m.root
      result
    }
  }
}

class Map[K,V](val context : Context[K,V], var root : Lazy[TrieNode[K,V]]) extends Iterable[(K,V)] {
  def empty = new Map(context, TrieEmpty.instance)
  
  def emptyMap[T] : Map[K,T] =
    new Map(Context(context.branchWidth, context.key, context.emptyBranch.as[T], context.offset), TrieEmpty.instance)
    
  def diff = new MapDiff(Context(context.branchWidth, context.key, context.emptyBranch.as[Option[V] => Option[V]], context.offset))

  // Single-key update methods
  def put(k : K, v : V) =
    new Map(context, new OptimisticThunk(new PutValueLazy(root, context, context.offset, k, v)))
  def putConditional(k : K, v : V, condition : Lazy[Boolean]) =
    new Map(context, new OptimisticThunk(new PutValueConditionalLazy(root, context, context.offset, k, v, condition)))
  def replace(k : K, f : Option[V] => Option[V]) = 
    //new Map(context, root.get.update(context, context.offset, k, f))
    new Map(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k, f)))

  def update(k : K, v : V) : Unit = { root = root.get.putInPlace(context, context.offset, k, v) }
  def update(k : K, f : Option[V] => Option[V]) : Unit = { root = root.get.updateInPlace(context, context.offset, k, f) }

  // Bulk update methods
  def write(diff : Map[K,V]) =
    new Map(context, new OptimisticThunk(new WriteDiffLazy(root, context, context.offset, diff.root)))
  def writeConditional(diff : Map[K,V], condition : Lazy[Boolean]) =
    new Map(context, new OptimisticThunk(new WriteDiffConditionalLazy(root, context, context.offset, diff.root, condition)))
  def update(diff : Map[K, Option[V] => Option[V]]) = {
    //new Map(context, root.get.update(context, context.offset, diff.root, diff.context))
    new Map(context, new OptimisticThunk(new UpdateDiffLazy(root, context, context.offset, diff.root, diff.context)))
  }
  
  def updateForceRoot(diff : Map[K, Option[V] => Option[V]]) = 
    new Map(context, root.get.update(context, context.offset, diff.root, diff.context))
    
  def map[W](f : (K,V) => Option[W]) =
    new Map(Context(context.branchWidth, context.key, context.emptyBranch.as[W], context.offset), new OptimisticThunk(new MapLazy(root, context, context.offset, f)))
  
  def mergeDD[W,X](other : Map[K,W], left : Option[(K,V) => Option[X]], right : Option[(K,W) => Option[X]], merge : (K,V,W) => Option[X]) = {
    val cx = Context(context.branchWidth, context.key, context.emptyBranch.as[X], context.offset)
    new Map(cx, new OptimisticThunk(new MergeLazyDD(root, other.root, context, other.context, cx, context.offset, left, right, merge)))
  }
  def mergeKD[W](other : Map[K,W], left : Option[(K,V) => Option[V]], right : Option[(K,W) => Option[V]], merge : (K,V,W) => Option[V]) =
    //new Map(context, root.get.mergeKD(other.root, context, other.context, context.offset, left, right, merge))
    new Map(context, new OptimisticThunk(new MergeLazyKD(root, other.root, context, other.context, context.offset, left, right, merge)))
  def mergeDK[W](other : Map[K,W], left : Option[(K,V) => Option[W]], right : Option[(K,W) => Option[W]], merge : (K,V,W) => Option[W]) =
    new Map(other.context, new OptimisticThunk(new MergeLazyDK(root, other.root, context, other.context, context.offset, left, right, merge)))
  def mergeKK(other : Map[K,V], left : Option[(K,V) => Option[V]], right : Option[(K,V) => Option[V]], merge : (K,V,V) => Option[V]) =
    new Map(context, new OptimisticThunk(new MergeLazyKK(root, other.root, context, other.context, context.offset, left, right, merge)))
  
  def updateRange(f : V => Option[V], low : K, high : K) = 
    new Map(context, new OptimisticThunk(new UpdateRangeLazy(root, context, context.offset, f, low, high)))
  def updateFrom(f : V => Option[V], low : K) = 
    new Map(context, new OptimisticThunk(new UpdateFromLazy(root, context, context.offset, f, low)))
  def updateUpto(f : V => Option[V], high : K) = 
    new Map(context, new OptimisticThunk(new UpdateUptoLazy(root, context, context.offset, f, high)))
  def updateAll(f : V => Option[V]) = 
    new Map(context, new OptimisticThunk(new UpdateAllLazy(root, context, context.offset, f)))

  def replaceRange(f : V => V, low : K, high : K) = 
    new Map(context, new OptimisticThunk(new ReplaceRangeLazy(root, context, context.offset, f, low, high)))
  def replaceFrom(f : V => V, low : K) = 
    new Map(context, new OptimisticThunk(new ReplaceFromLazy(root, context, context.offset, f, low)))
  def replaceUpto(f : V => V, high : K) = 
    new Map(context, new OptimisticThunk(new ReplaceUptoLazy(root, context, context.offset, f, high)))
  def replaceAll(f : V => V) = 
    new Map(context, new OptimisticThunk(new ReplaceAllLazy(root, context, context.offset, f)))
    
  def removeFrom(low : K, including : Boolean = true) =
    new Map(context, new OptimisticThunk(new RemoveFromLazy(root, context, context.offset, low, including)))
  def removeUpto(high : K, including : Boolean = false) =
    new Map(context, new OptimisticThunk(new RemoveUptoLazy(root, context, context.offset, high, including)))
  def removeRange(low : K, high : K, lowIncluding : Boolean = true, highIncluding : Boolean = false) =
    new Map(context, new OptimisticThunk(new RemoveRangeLazy(root, context, context.offset, low, high, lowIncluding, highIncluding)))
  
  // Maintenance methods
  def forceRoot = { getRoot; this }
  def force(k : K) : Unit = { getRoot.force(context, context.offset, k) }
  def forceRange(low : K, high : K) : Unit = getRoot.forceRange(context, context.offset, low, high)
  def forceFrom(low : K) : Unit = getRoot.forceFrom(context, context.offset, low)
  def forceUpto(high : K) : Unit = getRoot.forceUpto(context, context.offset, high)
  def forceAll() : Unit = getRoot.forceAll()
  def force(keys : Map[K,_]) = getRoot.forceKeys(context, context.offset, keys.root.get)
  def forceBranches(keys : Map[K,_]) = getRoot.forceBranches(context, context.offset, keys.root.get)
  
  /*
  def forceBF(keys : Map[K,_]) = {
    val q1 = new ArrayBuffer[TrieNode[K,V]]
    val q2 = new ArrayBuffer[TrieNode[K,_]]
    
    var q1p = 1
    var q2p = 1
    q1 += root.get
    q2 += keys.root.get
    
    while(q1p != 0) {
      val node = q1(q1p)
      val diff = q2(q2p)
      q1p -= 1
      q2p -= 1
      
      node match {
        case b@TrieBranch(nc) => diff match {
          case TrieBranch(dc) => {
            
          }
          case TrieLeaf(k,_) => {
            b.force(k)
          }
          case _ => {}
        }
        case _ => {}
      }
    }
  }
  */
  
  def compact = { root = getRoot.compact(context) }
  
  // Convenience methods
  def put(k : K, v : () => Option[V]) : Map[K,V] = replace(k, _ => v())
  def modify(k : K, f : V => V) : Map[K,V] = replace(k, (ov : Option[V]) => ov match {
    case Some(v) => Some(f(v))
    case None => None
  })
  def remove(k : K) : Map[K,V] = put(k, () => None)
  def filter(f : V => Boolean) = vmap(v => if(f(v)) Some(v) else None)
  def vmap[W](f : V => Option[W]) = map((_, v) => f(v))
  
  // Reading methods
  def apply(k : K) = get(k).get
  def get(k : K) : Option[V] = getRoot.get(context, context.offset, k)
  def floor(k : K) = getRoot.floor(context, context.offset, k)
  def ceil(k : K) = getRoot.ceil(context, context.offset, k)
  def prev(k : K) = getRoot.prev(context, context.offset, k)
  def next(k : K) = getRoot.next(context, context.offset, k)
  def firstEntry = getRoot.first
  def lastEntry = getRoot.last
  
  def foreach(f : (K,V) => Unit) = getRoot.foreach(f)
  
  def reduceAll[T](accum : T, f : (T,V) => T) = root.get.reduceAll(accum, f)
  def reduceFrom[T](accum : T, f : (T,V) => T, low : K) : T = root.get.reduceFrom(context, context.offset, accum, f, low)
  def reduceUpto[T](accum : T, f : (T,V) => T, high : K) : T = root.get.reduceUpto(context, context.offset, accum, f, high)
  def reduceRange[T](accum : T, f : (T,V) => T, low : K, high : K) : T = root.get.reduceRange(context, context.offset, accum, f, low : K, high : K)
  
  private def getRoot = {
    val c = root
    val ec = root.get
    if(!(c eq ec))
      root = ec
    ec
  }
  
  // Iterator
  override def iterator = new Iterator[(K,V)] {
    val stack = new LinkedList[Lazy[TrieNode[K,V]]]()
    var nxt : (K,V) = null
   
    stack.push(root)
    findNext()
    
    override def hasNext() = {
      nxt != null
    }
    
    override def next() = {
      val r = nxt
      nxt = null
      findNext()
      r
    }
    
    def findNext() = {
      while(nxt == null && !stack.isEmpty()) {
        stack.pop().get match {
          case TrieEmpty() => {}
          case TrieLeaf(k,v) => { nxt = (k,v) } 
          case TrieBranch(cs) => 
            var i = cs.length - 1
            while(i >= 0) {
              if(!(cs(i) eq TrieEmpty._instance)) {
                stack.push(cs(i))
              }
              i -= 1
            }
        }
      }
    }
  }
  
  // Diagnostic methods
  def printDebug = root.get.printDebug
}

object IntMap {
  implicit def toIntMap[V](map : Map[Int,V]) = IntMap(map) 
}

case class IntMap[V](val map : Map[Int,V]) {
  // Key transform methods
  def shift(n : Int) =
    new Map(map.context, new OptimisticThunk(new ShiftLazy(map.root, map.context, map.context.offset, 0, n)))
}

object ComposableMap {
  implicit def toComposableMap[K,V](map : Map[K,Option[V] => Option[V]]) = ComposableMap(map)
}

case class ComposableMap[K,V](val map : Map[K,Option[V] => Option[V]]) {  
  def composeInPlace(k : K, f : Option[V] => Option[V]) : Unit =
    map(k) = (x : Option[Option[V] => Option[V]]) => x match {
      case Some(current) => Some(f compose current)
      case None => Some(f)
    }
    
  def modifyInPlace(k : K, f : V => V) : Unit =
    composeInPlace(k, _ match {
      case Some(v) => Some(f(v))
      case None => throw new RuntimeException("Value is not present")
    })
}

object ComposableDiff {
  implicit def toComposableDiff[K,V](map : Map[K,V => V]) = ComposableDiff(map)
}

case class ComposableDiff[K,V](val map : Map[K,V => V]) {  
  def composeInPlace(k : K, f : V => V) : Unit =
    map(k) = (x : Option[V => V]) => x match {
      case Some(current) => Some(f compose current)
      case None => Some(f)
    }
    
  def modifyInPlace(k : K, f : V => V) : Unit = composeInPlace(k, f(_))
}
