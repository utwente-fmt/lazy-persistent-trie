package misc

object Transform {
  def main(args : Array[String]) = {
    while(true) {
      val in = readLine()
      val parts = in.split(" ")
      
      var i = 1
      for(p <- parts) {
        println("(" + i + "," + (p.toDouble / 1.0) + ")")
        i *= 2
      }
    }
  }
}