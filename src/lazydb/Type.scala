package lazydb

abstract class Type
case class Nominal(name : String) extends Type
case class Application(name : String, args : List[Type]) extends Type
case class Record(fields : List[(String, Type)]) extends Type