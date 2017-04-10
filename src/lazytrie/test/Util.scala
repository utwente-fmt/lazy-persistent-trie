package lazytrie.test

import lazytrie.SmallIntKey
import lazytrie.Map
import java.util.concurrent.ThreadLocalRandom
import lazytrie.StringKey
import scala.reflect.ClassTag

object Util {
  def createIntMap(size : Int, branchWidth : Int = 5) = {
    val rnd = ThreadLocalRandom.current()
    var map = Map.emptyInt[Int](size, branchWidth)
    for(i <- 0 to size - 1) {
      map(i) = rnd.nextInt(1000000)
    }
    map
  }
  
  def createStringMap(size : Int, branchWidth : Int = 5) = {
    val rnd = ThreadLocalRandom.current()
    val key = new StringKey()
    var map = Map[String,Int](branchWidth)(key)
    for(i <- 0 to size - 1) {
      map(i.toString) = rnd.nextInt(1000000)
    }
    map
  }
  
  def skewedRandom(max : Int, p : Double) : Int = {
    val rnd = ThreadLocalRandom.current()
    var low = 0
    var high = max - 1
    while(low != high) {
    	if(rnd.nextDouble() < p) {
    		high = (low + high) / 2
    	} else {
    		low = (low + high + 1) / 2
    	}
    }
    low
  }
}