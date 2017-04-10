package lazytrie.test

import java.util.Random
import lazytrie.IntKey
import laziness.Lazy

object Main {
  def main(args : Array[String]) = {
    test
  }
  
  def test = {
    val size = 100
    var i = 0
    
    while(true) {
      val rnd = new Random()
      
      var trie = Util.createIntMap(0, 1 + rnd.nextInt(4))
      var reference = Map[Int,Int]()
      
      for(_ <- 1 to 100) {
        var beforeTrie = trie
        var beforeReference = reference
        var operation = ""
        
        rnd.nextInt(13) match {
          // put(k, v)
          case 0 => 
            val k = rnd.nextInt(size)
            val v = rnd.nextInt(1000000)
            operation = "put(" + k + ", " + v + ")"
            trie = trie.put(k, v)
            reference = reference + (k -> v)
            
          // updateInPlace(k, v)
          case 1 => 
            val k = rnd.nextInt(size)
            val v = rnd.nextInt(1000000)
            operation = "putInPlace(" + k + ", " + v + ")"
            beforeTrie = trie.vmap(Some(_)) // Copies the tree
            beforeTrie.forceAll()
            trie(k) = _ => Some(v)
            reference = reference + (k -> v)
            
          // remove(k)
          case 2 =>
            val k = rnd.nextInt(size)
            operation = "remove(" + k + ")"
            trie = trie.remove(k)
            reference = reference - k
            
          // get(k)
          case 3 =>
            operation = "get(k)"
            for((k,v) <- trie) {
              if(trie.get(k).get != v)
                throw new Exception("Test failed")
            }
            
          // map(_ + 1)
          case 4 =>
            operation = "map(_ + 1)"
            trie = trie.map((k : Int, v : Int) => Some(v + 1))
            reference = reference.map({ case (k,v) => (k, v + 1) })
            
          // reduceAll
          case 5 =>
            operation = "reduceAll"
            val a = trie.reduceAll(0, (a : Int, b : Int) => a + b)
            val b = reference.foldLeft(0)({ case (a,(k,v)) => a + v })
            if(a != b)
              throw new Exception("Test failed")
            
          // reduceRange
          case 6 =>
            val low = rnd.nextInt(size)
            val high = low + rnd.nextInt(size - low)
            operation = "reduceRange(" + low + "," + high + ")"
            val a = trie.reduceRange(0, (a : Int, b : Int) => a + b, low, high)
            val b = reference.foldLeft(0)({ case (a,(k,v)) => if(k >= low && k <= high) a + v else a })
            if(a != b)
              throw new Exception("Test failed")
            
          // updateRange
          case 7 =>
            val low = rnd.nextInt(size)
            val high = low + rnd.nextInt(size - low)
            operation = "updateRange(" + low + "," + high + ")"
            trie = trie.updateRange((v : Int) => Some(v + 1), low, high)
            reference = reference.map({ case (k,v) => if(k >= low && k <= high) (k, v + 1) else (k, v) })
            
          // updateAll
          case 8 =>
            operation = "updateAll(_ + 1)"
            trie = trie.updateAll((v : Int) => Some(v + 1))
            reference = reference.map({ case (k,v) => (k, v + 1) })
            
          // updateDiff
          case 9 => 
            operation = "updateDiff"
            var updates = Map[Int,Int]()
            var diff = trie.emptyMap[Option[Int] => Option[Int]]
            for(i <- 1 to rnd.nextInt(100)) {
              val k = rnd.nextInt(size)
              val v = rnd.nextInt(1000000)
              updates += k -> v
              diff(k) = (_ : Option[Int]) => Some(v)
            }
            trie = trie.update(diff)
            reference = reference ++ updates

          // writeDiff
          case 10 =>
            var updates = Map[Int,Int]()
            var diff = trie.empty
            for(i <- 1 to rnd.nextInt(5) + 1) {
              val k = rnd.nextInt(size)
              val v = rnd.nextInt(1000000)
              updates += k -> v
              diff = diff.put(k, v)
            }
            operation = "writeDiff " + updates
            trie = trie.write(diff)
            reference = reference ++ updates
            
          // WriteConditional false
          case 11 =>
            var updates = Map[Int,Int]()
            var diff = trie.empty
            for(i <- 1 to rnd.nextInt(5) + 1) {
              val k = rnd.nextInt(size)
              val v = rnd.nextInt(1000000)
              updates += k -> v
              diff = diff.put(k, v)
            }
            operation = "writeConditionalDiff(false) " + updates
            trie = trie.writeConditional(diff, Lazy.optimistic(() => false))
            
          // WriteConditional true
          case 12 =>
            var updates = Map[Int,Int]()
            var diff = trie.empty
            for(i <- 1 to rnd.nextInt(5) + 1) {
              val k = rnd.nextInt(size)
              val v = rnd.nextInt(1000000)
              updates += k -> v
              diff = diff.put(k, v)
            }
            operation = "writeConditionalDiff(true) " + updates
            trie = trie.writeConditional(diff, Lazy.optimistic(() => true))
            reference = reference ++ updates
        }
        
        println(i + " " + operation)
        if(reference != trie.toMap) {
          println("Test failed")
          println(" " + beforeTrie + " -> " + trie)
          println(" " + beforeReference + " -> " + reference)
          System.exit(0)
        } else {
          i += 1
        }
      }
    }
  }
}