package lazydb

import java.io.StringReader

object Repl {
  def main(args : Array[String]) = {
    val parser = new Parser()
    
    val input = scala.io.Source.fromFile("test/quiz.ldb").mkString
    val preprocessed = Preprocessor.preprocess(input)
    
    val result = parser.parseAll(parser.transaction, new StringReader(preprocessed))
    println(result)
  }
}