package lazytrie.test

import lazytrie.Map
import lazytrie.SmallIntKey
import lazytrie.IntMap
import java.util.concurrent.ThreadLocalRandom

object MergeTest {
  val SIZE = 1000000
  
  def main(args : Array[String]) : Unit = {
    
    var va = Util.createIntMap(2)
    va(0) = 1
    va(1) = _ => None
    //va(2) = 1
 
    var vb = Util.createIntMap(2)
    vb(0) = _ => None
    vb(1) = _ => None
    //vb(2) = _ => None
    
    val r = va.mergeDK(vb, Some((k : Int, x : Int) => Some(x)), None, (k : Int, x : Int, y : Int) => Some(x + y))
    
    println(r)

    
  }
}