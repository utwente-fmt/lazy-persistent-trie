package lazytrie

import laziness.OptimisticThunk
import java.util.LinkedList
import laziness.Lazy

object Set {  
  def apply[K](branchWidth : Int = 5)(implicit key : Key[K]) : Set[K] = {
    val emptyBranch = new TrieBranch[K,Unit](Array.ofDim[TrieNode[K,Unit]](1 << branchWidth).map(_ => TrieEmpty.instance[K,Unit]))
    
    // Compute how many zero bits need to be padded at the root to align the leafs
    // E.g. if we have 21 bits, and width 4, the leaves would store only 1 bit, which wastes a lot of memory
    // To solve this, we need to pad 3 bits at the root, and we compute the initial offset as -3
    val branchBits = key.maxBits
    var offset = 0
    val roundedDown = ((branchBits / branchWidth) * branchWidth)
    if(key.maxBits >= 0 && roundedDown != branchBits)
      offset = (branchBits - roundedDown) - branchWidth

    new Set(Context(branchWidth, key, emptyBranch, offset), TrieEmpty.instance)
  }
}

class Set[K](val context : Context[K,Unit], var root : Lazy[TrieNode[K,Unit]]) extends Iterable[K] {
  private def transform(f : Boolean => Boolean) = (c : Option[Unit]) => c match {
      case Some(()) => if(f(true)) Some(()) else None
      case None => if(f(false)) Some(()) else None
    }
  
  def empty = new Set[K](context, TrieEmpty.instance)
  //def emptyDiff(implicit ftag : ClassTag[Boolean => Boolean]) : Map[K,Boolean => Boolean] =
  //  new Map(Context(context.leafWidth, context.branchWidth, context.key, context.emptyBranch, context.ktag, ftag, context.offset), TrieEmpty.instance)
    
  // Single-key update methods
  def add(k : K) =
    new Set(context, new OptimisticThunk(new PutValueLazy(root, context, context.offset, k, ())))
  def addConditional(k : K, condition : Lazy[Boolean]) =
    new Set(context, new OptimisticThunk(new PutValueConditionalLazy(root, context, context.offset, k, (), condition)))
  def replace(k : K, f : Boolean => Boolean) = 
    new Set(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k, transform(f))))
  def +=(k : K) = { root = getRoot.putInPlace(context, context.offset, k, ()) }
    
  def update(k : K, f : Boolean => Boolean) : Unit = { root = getRoot.updateInPlace(context, context.offset, k, transform(f)) }

  // Bulk update methods
  def write(diff : Set[K]) =
    new Set(context, new OptimisticThunk(new WriteDiffLazy(root, context, context.offset, diff.root)))
  def writeConditional(diff : Set[K], condition : Lazy[Boolean]) =
    new Set(context, new OptimisticThunk(new WriteDiffConditionalLazy(root, context, context.offset, diff.root, condition)))
  //def update(diff : Map[K, Boolean => Boolean]) =
  //  new Set(context, new OptimisticThunk(new UpdateDiffLazy(root, context, context.offset, diff.root, diff.context)))
  
  def filter[W](f : K => Boolean) =
    new Set(context, new OptimisticThunk(new MapLazy(root, context, context.offset, (k : K, _ : Unit) => f(k) match { case true => Some(()) case false => None })))
  
  // Maintenance methods
  def forceRoot = { getRoot; this }
  def force(k : K) : Unit = { getRoot.force(context, context.offset, k) }
  def forceRange(low : K, high : K) : Unit = getRoot.forceRange(context, context.offset, low, high)
  def forceFrom(low : K) : Unit = getRoot.forceFrom(context, context.offset, low)
  def forceUpto(high : K) : Unit = getRoot.forceUpto(context, context.offset, high)
  def forceAll() : Unit = getRoot.forceAll()
  
  def compact = { root = getRoot.compact(context) }
  
  // Convenience methods
  def put(k : K, v : () => Boolean) : Set[K] = replace(k, _ => v())
  def remove(k : K) = new Set(context, new OptimisticThunk(new UpdateValueLazy(root, context, context.offset, k, (_ : Option[Unit]) => None)))
  
  // Reading methods
  def contains(k : K) = getRoot.get(context, context.offset, k).isDefined
  def floor(k : K) : Option[K] = getRoot.floor(context, context.offset, k).map(_._1)
  def ceil(k : K) : Option[K] = getRoot.ceil(context, context.offset, k).map(_._1)
  def prev(k : K) : Option[K] = getRoot.prev(context, context.offset, k).map(_._1)
  def next(k : K) : Option[K] = getRoot.next(context, context.offset, k).map(_._1)
  def firstKey = getRoot.first.map(_._1)
  def lastKey = getRoot.last.map(_._1)
  
  private def getRoot = {
    val c = root
    val ec = root.get
    if(!(c eq ec))
      root = ec
    ec
  }
  
  // Iterator
  override def iterator = new Iterator[K] {
    val stack = new LinkedList[Lazy[TrieNode[K,Unit]]]()
    var nxt = new LinkedList[K]
   
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
          case TrieLeaf(k,v) => { nxt.push(k) } 
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