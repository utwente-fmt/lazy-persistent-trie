package lazytrie.test

import lazytrie.SmallIntKey
import lazytrie.Map
import java.util.concurrent.ThreadLocalRandom
import lazytrie.StringKey

object Util {
  def createIntMap(size : Int, leafWidth : Int = 0, branchWidth : Int = 5) = {
    val rnd = ThreadLocalRandom.current()
    val discr = new SmallIntKey(Math.ceil(Math.log(size) / Math.log(2)).toInt)
    var map = Map[Int,Int](discr, leafWidth, branchWidth)
    for(i <- 0 to size - 1) {
      map.putInPlace(i, rnd.nextInt(1000000))
    }
    map
  }
  
  def createStringMap(size : Int, leafWidth : Int = 0, branchWidth : Int = 5) = {
    val rnd = ThreadLocalRandom.current()
    val discr = new StringKey()
    var map = Map[String,Int](discr, leafWidth, branchWidth)
    for(i <- 0 to size - 1) {
      map.putInPlace(i.toString, rnd.nextInt(1000000))
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