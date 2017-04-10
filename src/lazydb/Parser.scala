package lazydb

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.combinator.ImplicitConversions
import scala.util.parsing.combinator.JavaTokenParsers
import scala.annotation.migration

class Parser extends JavaTokenParsers with RegexParsers with ImplicitConversions {
  protected override val whiteSpace = """(\n|\r| |\t|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r
  
  lazy val identLower : Parser[String] = """[a-z_]\w*""".r
  lazy val identUpper : Parser[String] = """[A-Z_]\w*""".r

  lazy val string : Parser[String] = stringLiteral ^^ {str => str.substring(1, str.length - 1)}
  lazy val integer : Parser[Int] = """-?\d+""".r ^^ { _.toInt }
  lazy val double : Parser[Double] = """-?\d+(\.\d*)""".r ^^ { _.toDouble }
  
  def left[T](p : Parser[T], q : Parser[T => T]) : Parser[T] =
    p ~ q.* ^^ { case a ~ b => (a /: b)((acc,f) => f(acc)) }

  def prefix[T,U](p : Parser[T], q : Parser[T => U]) : Parser[U] =
    p ~ q ^^ { case a ~ b => b(a) }
  
  lazy val transaction : Parser[Transaction] =
    "BEGIN_BLOCK" ~> (txstatement <~ "LINE_END".?).* <~ "END_BLOCK" ^^ Transaction
    
  // TODO: factor out common prefixes
  lazy val txstatement : Parser[TxStatement] =
    "def" ~> identUpper ~ ("(" ~> repsep(identLower ~ (":" ~> typeDef) ^^ Pair[String,Type], ",") <~ ")") ^^ DefineRecordStatement |
    "def" ~> identUpper ~ ("." ~> identLower) ~ ("(" ~> repsep(identLower, ",") <~ ")") ~ block ^^ DefineProcedureStatement |
    "drop" ~> identUpper ~ ("." ~> identLower) ^^ DropStatement |
    "drop" ~> identUpper ^^ DropTypeStatement |
    statement
    
  lazy val block : Parser[List[Statement]] =
    "LINE_END" ~> "BEGIN_BLOCK" ~> (statement <~ "LINE_END".?).* <~ "END_BLOCK"
    
  lazy val statement : Parser[Statement] =
    "val" ~> identLower ~ ("=" ~> expression) ^^ LetStatement |
    "return" ~> expression ^^ ReturnStatement |
    "abort" ~> expression ^^ AbortStatement |
    "if" ~> expression ~ block ~ ("else" ~> block).? ^^ IfStatement |
    location ~ ("(" ~> repsep(expression, ",") <~ ")") ^^ { case n ~ ps => CallStatement(None, n, ps) } |
    prefix(location, 
      "=" ~> expression ^^ { case x => AssignStatement(_ : Location, x) } |
      "@=" ~> expression ^^ { case x => UpdateStatement(_ : Location, x) } |
      "<-" ~> location ~ ("(" ~> repsep(expression, ",") <~ ")") ^^ { case n ~ ps => x : Location => CallStatement(Some(x), n, ps) }
    )
    
  lazy val expression : Parser[Expression] =
    "{" ~> block <~ "}" ^^ UpdateExpression |
    identLower.* ~ ("->" ~> expression) ^^ ClosureExpression |
    left(leafExpression,
      "(" ~> repsep(expression, ",") <~ ")" ^^ { case x => ApplicationExpression(_ : Expression, x) }
    )
    
  lazy val leafExpression : Parser[Expression] =
    "(" ~> expression <~ ")" |
    "if" ~> expression ~ ("then" ~> expression) ~ ("else" ~> expression) ^^ IfExpression |
    literal ^^ LiteralExpression |
    location ^^ VariableExpression
    
  lazy val literal : Parser[Literal] =
    integer ^^ IntLiteral |
    double ^^ DoubleLiteral |
    string ^^ StringLiteral
    
  lazy val location : Parser[Location] =
    left(leafLocation,
      "." ~> identLower ^^ { case x => MemberLocation(_ : Location, x) } |
      "[" ~> expression <~ "]" ^^ { case x => IndexLocation(_ : Location, x) }
    )
    
  lazy val leafLocation : Parser[Location] =
    identLower ^^ VariableLocation |
    identUpper ^^ TypeLocation
    
  lazy val typeDef : Parser[Type] =
    identUpper ~ ("[" ~> repsep(typeDef, ",") <~ "]") ^^ Application |
    identUpper ^^ Nominal //| 
    //"fun" ~> typeDef.* ~ ("->" ~> typeDef) ^^ TypeFunction 
}

object Preprocessor {
    val BlockStartToken = "BEGIN_BLOCK"
    val BlockEndToken = "END_BLOCK"
    val LineEndToken = "LINE_END"

    val TabSize = 4 // how many spaces does a tab take

    def preprocess(text : String): String = {
        val lines = text.split('\n').toList.filterNot(_.forall(isWhiteChar)).map(_ + " " + LineEndToken)
        val processedLines = BlockStartToken :: insertTokens(lines, List(0))
        processedLines.mkString("\n")
    }

    def insertTokens(lines: List[String], stack: List[Int]): List[String] = lines match {
        case List() => List.fill(stack.length) { BlockEndToken } //closing all opened blocks
        case line :: rest => {
            (computeIndentation(line), stack) match {
                case (indentation, top :: stackRest) if indentation > top => {
                    BlockStartToken :: line :: insertTokens(rest,  indentation :: stack)
                }
                case (indentation, top :: stackRest) if indentation == top =>
                    line :: insertTokens(rest, stack)
                case (indentation, top :: stackRest) if indentation < top => {
                    BlockEndToken :: insertTokens(lines, stackRest)
                }
                case _ => throw new IllegalStateException("Invalid algorithm")
            }
        }
    }


    private def computeIndentation(line: String): Int = {
        val whiteSpace = line takeWhile isWhiteChar
        (whiteSpace map {
            case ' ' => 1
            case '\t' => TabSize
        }).sum
    }

    private def isWhiteChar(ch: Char) = ch == ' ' || ch == '\t'
}