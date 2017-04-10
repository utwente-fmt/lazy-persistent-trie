package lazytx.benchmark.tpcc

import java.util.concurrent.ThreadLocalRandom

object Util {
  val nameParts = Array("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING")
  var names = Array.ofDim[String](1000)
  for(i <- 0 to 999) {
    names(i) = genname(i)
  }
  
  def c(a : Int) = rand(0, ThreadLocalRandom.current.nextInt(a))
  
  def rand(a : Int, b : Int) : Int = ThreadLocalRandom.current().nextInt(b - a + 1) + a
  
  def randa(length : Int) : String = {
    var result = new StringBuilder(length)
    val rnd = ThreadLocalRandom.current
    for(i <- 1 to length) {
      result.append(String.valueOf(('a' + rnd.nextInt(26)).toChar))
    }
    result.toString()
  }
  
  def randOriginal() : String = {
    var r = randa(rand(26, 50))
    if(rand(1, 10) == 1) {
      r
    } else {
      val start = r.length() - 8
      r.substring(0, start) + "ORIGINAL" + r.substring(start + 8, r.length)
    }
  }
  
  def nurand(a : Int, x : Int, y : Int) = (((rand(0, a) | rand(x, y)) + c(a)) % (y - x + 1)) + x
  
  def genname(n : Int) = nameParts(n / 100) + nameParts((n / 10) % 10) + nameParts(n % 10)
  
  def name(n : Int) = names(n)
  
  def permutation(count : Int) = {
    var result = Array.ofDim[Int](count)
    val rnd = ThreadLocalRandom.current
    for(i <- 1 to count) {
      result(i - 1) = i
    }
    for(i <- 0 to count - 1) {
      val temp = result(i)
      val s = i + (rnd.nextInt(count - i)).toInt
      result(i) = result(s)
      result(s) = temp
    }
    result
  }
  
  def new_order_key(w_id : Long, d_id : Long, o_id : Long) =
    (w_id << 40) + (d_id << 32) + o_id
}