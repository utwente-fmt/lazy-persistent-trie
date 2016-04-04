package lazytrie

import java.util.LinkedList
import scala.reflect.ClassTag
import laziness.OptimisticThunk
import laziness.Lazy

object Map {  
  def apply[K,V](discr : Key[K], leafWidth : Int = 0, branchWidth : Int = 5)(implicit ktag : ClassTag[K], vtag : ClassTag[V]) : Map[K,V] = {
    val emptyBranch = new TrieBranch[K,V](Array.ofDim[TrieNode[K,V]](1 << branchWidth).map(_ => TrieEmpty.instance[K,V]))
    
    // Compute how many zero bits need to be padded at the root to align the leafs
    // E.g. if we have 21 bits, and width 4, the leaves would store only 1 bit, which wastes a lot of memory
    // To solve this, we need to pad 3 bits at the root, and we compute the initial offset as -3
    val branchBits = discr.maxBits - leafWidth
    var offset = 0
    val roundedDown = ((branchBits / branchWidth) * branchWidth)
    if(discr.maxBits >= 0 && roundedDown != branchBits)
      offset = (branchBits - roundedDown) - branchWidth

    new Map(Context(leafWidth, branchWidth, discr, emptyBranch, ktag, vtag, offset), TrieEmpty.instance)
  }
}

class Map[K,V](val context : Context[K,V], var root : Lazy[TrieNode[K,V]]) extends Iterable[(K,V)] {
  def empty = new Map(context, TrieEmpty.instance)
  def emptyDiff(implicit ftag : ClassTag[Option[V] => Option[V]]) : Map[K,Option[V] => Option[V]] =
    new Map(Context(context.leafWidth, context.branchWidth, context.key, context.emptyBranch.as[Option[V] => Option[V]], context.ktag, ftag, context.offset), TrieEmpty.instance)
    
  // Single-key update methods
  def put(k : K, v : V) =
    new Map(context, new OptimisticThunk(new PutValueLazy(root, context, context.offset, k, v)))
  def putConditional(k : K, v : V, condition : Lazy[Boolean]) =
    new Map(context, new OptimisticThunk(new PutValueConditionalLazy(root, context, context.offset, k, v, condition)))
  def putInPlace(k : K, v : V) : Unit = { root = root.get.putInPlace(context, context.offset, k, v) } 
  def update(k : K, f : Option[V] => Option[V]) = 
    new Map(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k, f)))
  def updateInPlace(k : K, f : Option[V] => Option[V]) : Unit = { root = root.get.updateInPlace(context, context.offset, k, f) }
  
  // Bulk update methods
  def write(diff : Map[K,V]) =
    new Map(context, new OptimisticThunk(new WriteDiffLazy(root, context, context.offset, diff.root)))
  def writeConditional(diff : Map[K,V], condition : Lazy[Boolean]) =
    new Map(context, new OptimisticThunk(new WriteDiffConditionalLazy(root, context, context.offset, diff.root, condition)))
  def update(diff : Map[K, Option[V] => Option[V]]) =
    new Map(context, new OptimisticThunk(new UpdateDiffLazy(root, context, context.offset, diff.root, diff.context)))
  
  def map[W](f : (K,V) => Option[W])(implicit wtag : ClassTag[W]) =
    new Map(Context(context.leafWidth, context.branchWidth, context.key, context.emptyBranch.as[W], context.ktag, wtag, context.offset), new OptimisticThunk(new MapLazy(root, context, context.offset, f)))
  
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
    
  // Maintenance methods
  def forceRoot : Unit = { root = root.get }
  def force(k : K) : Unit = { root.get.force(context, context.offset, k) }
  def forceRange(low : K, high : K) : Unit = root.get.forceRange(context, context.offset, low, high)
  def forceFrom(low : K) : Unit = root.get.forceFrom(context, context.offset, low)
  def forceUpto(high : K) : Unit = root.get.forceUpto(context, context.offset, high)
  def forceAll() : Unit = root.get.forceAll()
  
  def compact = { root = root.get.compact(context) }
  
  // Convenience methods
  def put(k : K, v : () => Option[V]) : Map[K,V] = update(k, _ => v())
  def modify(k : K, f : V => V) : Map[K,V] = update(k, (ov : Option[V]) => ov match {
    case Some(v) => Some(f(v))
    case None => None
  })
  def remove(k : K) : Map[K,V] = put(k, () => None)
  def filter(f : V => Boolean) = vmap(v => if(f(v)) Some(v) else None)(context.vtag)
  def vmap[W:ClassTag](f : V => Option[W]) = map((_, v) => f(v))
  
  // Reading methods
  def get(k : K) = root.get.get(context, context.offset, k)
  
  def reduceAll[T](accum : T, f : (T,V) => T) = root.get.reduceAll(accum, f)
  def reduceFrom[T](accum : T, f : (T,V) => T, low : K) : T = root.get.reduceFrom(context, context.offset, accum, f, low)
  def reduceUpto[T](accum : T, f : (T,V) => T, high : K) : T = root.get.reduceUpto(context, context.offset, accum, f, high)
  def reduceRange[T](accum : T, f : (T,V) => T, low : K, high : K) : T = root.get.reduceRange(context, context.offset, accum, f, low : K, high : K)
  
  // Iterator
  override def iterator = new Iterator[(K,V)] {
    val stack = new LinkedList[Lazy[TrieNode[K,V]]]()
    var nxt = new LinkedList[(K,V)]
   
    stack.push(root)
    findNext()
    
    override def hasNext() = {
      !nxt.isEmpty
    }
    
    override def next() = {
      val r = nxt.pop()
      findNext()
      r
    }
    
    def findNext() = {
      while(nxt.isEmpty() && !stack.isEmpty()) {
        stack.pop().get match {
          case TrieEmpty() => {}
          case TrieLeaf(k,v) => { nxt.push((k,v)) } 
          case WideLeaf(ks,vs,end) => {
            var i = end
            while(i > 0) {
              i -= 1
              nxt.addFirst((ks(i), vs(i)))
            }
          }
          case TrieBranch(cs) => 
            var i = cs.length - 1
            while(i >= 0) {
              stack.push(cs(i))
              i -= 1
            }
        }
      }
    }
  }
  
  // Diagnostic methods
  def printDebug = root.get.printDebug
}