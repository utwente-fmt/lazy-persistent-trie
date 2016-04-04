package lazytrie

trait Key[T] {
  def maxBits : Int
  def partition(value : T, offset : Int, length : Int) : Int
  def eq(a : T, b : T) : Boolean
  def compare(a : T, b : T) : Int
}

object IntKey {
  val instance = new IntKey
}

class IntKey extends Key[Int] {
  def maxBits = 32
  def partition(value : Int, offset : Int, length : Int) = (value << offset) >>> (32 - length)
  def eq(a : Int, b : Int) = a == b
  def compare(a : Int, b : Int) = a - b
}

class SmallIntKey(final val bits : Int) extends Key[Int] {  
  def maxBits = bits
  def partition(value : Int, offset : Int, length : Int) = (value << offset << (32 - bits)) >>> (32 - length)
  def eq(a : Int, b : Int) = a == b
  def compare(a : Int, b : Int) = a - b
}

class LongKey extends Key[Long] {
  def maxBits = 64
  def partition(value : Long, offset : Int, length : Int) = ((value << offset) >>> (64 - length)).toInt
  def eq(a : Long, b : Long) = a == b
  def compare(a : Long, b : Long) = if(a == b) 0 else if(a < b) -1 else 1
}

class StringKey extends Key[String] {
  def maxBits = -1
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