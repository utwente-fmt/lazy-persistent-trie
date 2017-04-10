package lazytrie 

object KeyInstances {
  implicit val int : Key[Int] = new IntKey()
  implicit val long : Key[Long] = new LongKey()
  implicit val string : Key[String] = new StringKey()
  implicit def tuple2[A,B](a : Key[A], b : Key[B]) = new Tuple2Key(a,b)
  //implicit def tuple3[A,B,C](a : Key[A], b : Key[B], c : Key[C]) = new Tuple3Key(a,b,c)
}

trait Key[T] {
  def maxBits : Int
  def bits(v : T) : Int
  def partition(value : T, offset : Int, length : Int) : Int
  def eq(a : T, b : T) : Boolean
  def compare(a : T, b : T) : Int
}

class IntKey extends Key[Int] {
  def maxBits = 32
  def bits(v : Int) = 32
  def partition(value : Int, offset : Int, length : Int) = (value << offset) >>> (32 - length)
  def eq(a : Int, b : Int) = a == b
  def compare(a : Int, b : Int) = a - b
}

object SmallIntKey {
  def make(max : Int) = 
    new SmallIntKey(Math.ceil(Math.log(max) / Math.log(2)).toInt)
}

class SmallIntKey(final val bits : Int) extends Key[Int] {  
  def maxBits = bits
  def bits(v : Int) = bits
  def partition(value : Int, offset : Int, length : Int) = (value << offset << (32 - bits)) >>> (32 - length)
  def eq(a : Int, b : Int) = a == b
  def compare(a : Int, b : Int) = a - b
}

class LongKey extends Key[Long] {
  def maxBits = 64
  def bits(v : Long) = 64
  def partition(value : Long, offset : Int, length : Int) = ((value << offset) >>> (64 - length)).toInt
  def eq(a : Long, b : Long) = a == b
  def compare(a : Long, b : Long) = if(a == b) 0 else if(a < b) -1 else 1
}

class StringKey extends Key[String] {
  def maxBits = -1
  def bits(v : String) = v.length * 16
  def partition(value : String, offset : Int, length : Int) = {
    val startChar = offset / 16
    val l = value.length()
    
    var bits = 0
    if(startChar < l)
      bits |= value.charAt(startChar).toInt << 16
    if(startChar + 1 < l)
      bits |= value.charAt(startChar + 1).toInt
    
    (bits << (offset % 16)) >>> (32 - length)
  }
  def eq(a : String, b : String) = a == b
  def compare(a : String, b : String) = a.compareTo(b)
}

class CaseInsensitiveStringKey extends Key[String] {
  def maxBits = -1
  def bits(v : String) = v.length * 16
  def partition(value : String, offset : Int, length : Int) = {
    val startChar = offset / 16
    val l = value.length()
    
    var bits = 0
    if(startChar < l)
      bits |= value.charAt(startChar).toLower.toInt << 16
    if(startChar + 1 < l)
      bits |= value.charAt(startChar + 1).toLower.toInt
    
    (bits << (offset % 16)) >>> (32 - length)
  }
  def eq(a : String, b : String) = a.equalsIgnoreCase(b)
  def compare(a : String, b : String) = a.compareToIgnoreCase(b)
}

class Tuple2Key[A,B](a : Key[A], b : Key[B]) extends Key[(A,B)] {
  def maxBits =
    if(a.maxBits == -1 || b.maxBits == -1)
      -1
    else
      a.maxBits + b.maxBits
  
  def bits(v : (A,B)) = a.bits(v._1) + b.bits(v._2)
      
  def partition(v : (A,B), offset : Int, length : Int) = {
    val ab = a.bits(v._1)
    if(offset < ab) {
      if(offset + length > ab) {
        val alen = ab - offset
        val blen = length - alen
        val ap = a.partition(v._1, offset, alen)
        val bp = b.partition(v._2, 0, blen)
        (ap << blen) | bp
      } else {
        a.partition(v._1, offset, length)
      }
    } else {
      b.partition(v._2, offset - ab, length)
    }
  }
      
  def eq(x : (A,B), y : (A,B)) = a.eq(x._1, y._1) && b.eq(x._2, y._2)
  
  def compare(x : (A,B), y : (A,B)) = {
    val r = a.compare(x._1, y._1)
    if(r == 0)
      b.compare(x._2, y._2)
    else
      r
   }
}

/*
class Tuple3Key[A,B,C](a : Key[A], b : Key[B], c : Key[C]) extends Key[(A,B,C)] {
  def maxBits =
    if(a.maxBits == -1 || b.maxBits == -1 || c.maxBits == -1)
      -1
    else
      a.maxBits + b.maxBits + c.maxBits
  
  def bits(v : (A,B,C)) = a.bits(v._1) + b.bits(v._2) + c.bits(v._3)
      
  def partition(v : (A,B,C), offset : Int, length : Int) = {
    val ab = a.bits(v._1)
    val bb = ab + b.bits(v._2)
    if(offset < ab) {
      if(offset + length > bb) {
        
      else if(offset + length > ab) {
        val alen = ab - offset
        val blen = length - alen
        val ap = a.partition(v._1, offset, alen)
        val bp = b.partition(v._2, 0, blen)
        (ap << blen) | bp
      } else {
        a.partition(v._1, offset, length)
      }
    } else {
      b.partition(v._2, offset - ab, length)
    }
  }
      
  def eq(x : (A,B,C), y : (A,B,C)) = a.eq(x._1, y._1) && b.eq(x._2, y._2) && c.eq(x._3, y._3)
  
  def compare(x : (A,B,C), y : (A,B,C)) = {
    val r = a.compare(x._1, y._1)
    if(r == 0) {
      val s = b.compare(x._2, y._2)
      if(s == 0) {
        c.compare(x._3, y._3)
      } else
        s
    } else
      r
   }
}
*/