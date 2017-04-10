package lazytrie

object HashMap {
  
}

/*
import scala.reflect.ClassTag
import laziness.OptimisticThunk
import java.util.LinkedList
import laziness.Lazy

object HashMap {  
  def apply[K,V](leafWidth : Int = 0, branchWidth : Int = 5) : HashMap[K,V] = {
    val emptyBranch = new TrieBranch[Int,HashLeaf[K,V]](Array.ofDim[TrieNode[Int,HashLeaf[K,V]]](1 << branchWidth).map(_ => TrieEmpty.instance[Int,HashLeaf[K,V]]))
    
    val key = KeyInstances.int
    
    // Compute how many zero bits need to be padded at the root to align the leafs
    // E.g. if we have 21 bits, and width 4, the leaves would store only 1 bit, which wastes a lot of memory
    // To solve this, we need to pad 3 bits at the root, and we compute the initial offset as -3
    val branchBits = key.maxBits - leafWidth
    var offset = 0
    val roundedDown = ((branchBits / branchWidth) * branchWidth)
    if(key.maxBits >= 0 && roundedDown != branchBits)
      offset = (branchBits - roundedDown) - branchWidth

    new HashMap(Context(leafWidth, branchWidth, key, emptyBranch, implicitly, implicitly, offset), TrieEmpty.instance)
  }
}

case class HashLeaf[K,V](key : K, value : V, next : HashLeaf[K,V]) {
  def put(k : K, v : V) : HashLeaf[K,V]
  def get(k : K) : Option[V]
}

class HashMap[K,V](val context : Context[Int,HashLeaf[K,V]], var root : Lazy[TrieNode[Int,HashLeaf[K,V]]]) extends Iterable[(K,V)] {
  def empty = new Map(context, TrieEmpty.instance)
  def emptyDiff(implicit ftag : ClassTag[Option[V] => Option[V]]) : Map[K,Option[V] => Option[V]] =
    new Map(Context(context.leafWidth, context.branchWidth, context.key, context.emptyBranch.as[Option[V] => Option[V]], context.ktag, ftag, context.offset), TrieEmpty.instance)
    
  // Single-key update methods
  def put(k : K, v : V) =
    new HashMap(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k.hashCode, (hl : Option[HashLeaf[K,V]]) => hl match {
      case None => Some(HashLeaf(k, v, null))
      case Some(hl) => Some(hl.put(k, v))
    })))  
  def putConditional(k : K, v : V, condition : Lazy[Boolean]) =
    new HashMap(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k, v, condition)))
  def replace(k : K, f : Option[V] => Option[V]) = 
    new HashMap(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k, f)))

  def update(k : K, v : V) : Unit = { root = getRoot.putInPlace(context, context.offset, k, v) }
  def update(k : K, f : Option[V] => Option[V]) : Unit = { root = getRoot.updateInPlace(context, context.offset, k, f) }

  // Bulk update methods
  def write(diff : Map[K,V]) =
    new HashMap(context, new OptimisticThunk(new WriteDiffLazy(root, context, context.offset, diff.root)))
  def writeConditional(diff : Map[K,V], condition : Lazy[Boolean]) =
    new HashMap(context, new OptimisticThunk(new WriteDiffConditionalLazy(root, context, context.offset, diff.root, condition)))
  def update(diff : Map[K, Option[V] => Option[V]]) =
    new HashMap(context, new OptimisticThunk(new UpdateDiffLazy(root, context, context.offset, diff.root, diff.context)))
  
  def map[W](f : (K,V) => Option[W]) =
    new HashMap(Context(context.leafWidth, context.branchWidth, context.key, context.emptyBranch.as[W], context.ktag, implicitly, context.offset), new OptimisticThunk(new MapLazy(root, context, context.offset, f)))
  
  // Maintenance methods
  def forceRoot = { getRoot; this }
  def force(k : K) : Unit = { getRoot.force(context, context.offset, k) }
  def forceRange(low : K, high : K) : Unit = getRoot.forceRange(context, context.offset, low, high)
  def forceFrom(low : K) : Unit = getRoot.forceFrom(context, context.offset, low)
  def forceUpto(high : K) : Unit = getRoot.forceUpto(context, context.offset, high)
  def forceAll() : Unit = getRoot.forceAll()
  
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
  def get(k : K) = getRoot.get(context, context.offset, k)
  def firstEntry = getRoot.first(context)
  def lastEntry = getRoot.last(context)
  
  def reduce[T](accum : T, f : (T,V) => T) = getRoot.reduceAll(accum, f)
  
  private def getRoot = {
    val c = root
    val ec = root.get
    if(!(c eq ec))
      root = ec
    ec
  }
  
  // Iterator
  override def iterator = new Iterator[(K,V)] {
    val stack = new LinkedList[Lazy[TrieNode[K,HashLeaf[K,V]]]]()
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
  def printDebug = getRoot.printDebug
}
*/