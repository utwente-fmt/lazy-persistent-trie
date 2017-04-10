package lazydb

case class Transaction(body : List[TxStatement])

abstract class TxStatement()
case class DefineRecordStatement(name : String, fields : List[(String, Type)]) extends TxStatement
case class DefineProcedureStatement(clazz : String, name : String, arguments : List[String], body : List[Statement]) extends TxStatement
case class DropStatement(clazz : String, name : String) extends TxStatement
case class DropTypeStatement(clazz : String) extends TxStatement

abstract class Statement() extends TxStatement
case class LetStatement(name : String, expression : Expression) extends Statement
case class AssignStatement(location : Location, expression : Expression) extends Statement
case class UpdateStatement(location : Location, expression : Expression) extends Statement
case class IfStatement(condition : Expression, ifTrue : List[Statement], ifFalse : Option[List[Statement]]) extends Statement
case class AbortStatement(message : Expression) extends Statement
case class ReturnStatement(value : Expression) extends Statement
case class CallStatement(result : Option[Location], location : Location, args : List[Expression]) extends Statement

abstract class Location
case class VariableLocation(name : String) extends Location
case class TypeLocation(name : String) extends Location
case class MemberLocation(source : Location, field : String) extends Location
case class IndexLocation(source : Location, index : Expression) extends Location

abstract class Expression
case class LiteralExpression(literal : Literal) extends Expression
case class ClosureExpression(args : List[String], body : Expression) extends Expression
case class ApplicationExpression(function : Expression, arguments : List[Expression]) extends Expression
case class VariableExpression(location : Location) extends Expression
case class IfExpression(condition : Expression, ifTrue : Expression, ifFalse : Expression) extends Expression
case class UpdateExpression(body : List[Statement]) extends Expression

abstract class Literal
case class IntLiteral(value : Int) extends Literal
case class DoubleLiteral(value : Double) extends Literal
case class StringLiteral(value : String) extends Literal
case class BooleanLiteral(value : Boolean) extends Literal
case class RecordLiteral(values : List[(String, Expression)]) extends Literal