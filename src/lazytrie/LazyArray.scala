package lazytrie

import laziness.Lazy
import laziness.Value
import scala.reflect.ClassTag

object LazyArray {
  def ofDim[T <: Lazy[T] : ClassTag](length : Int) = new LazyArray(Array.ofDim[Lazy[T]](length))
}

class LazyArray[T <: Lazy[T]](values : Array[Lazy[T]]) {
  def update(index : Int, v : T) : Unit = {
    values(index) = v
  }
  
  def update(index : Int, f : T => T) : LazyArray[T] = {
    var newValues = values.clone()
    val result = new LazyArray(newValues)
    val v = values(index)
    newValues(index) = Lazy.optimistic(() => f(v.get))
    result
  }
  
  def update(fs : Array[T => T]) : LazyArray[T] = {
    var newValues = values.clone()
    val result = new LazyArray(newValues)
    
    var i = 0
    while(i < values.length) {
      if(fs(i) != null) {
        val f = fs(i)
        val v = values(i)
        newValues(i) = Lazy.optimistic(() => f(v.get))
      }
      i += 1
    }
    
    result
  }
  
  def map(f : (Int,T) => T) : LazyArray[T] = {
    var newValues = values.clone()
    val result = new LazyArray(newValues)
    
    var i = 0
    while(i < values.length) {
      val index = i
      val v = values(i)
      if(v != null)
        newValues(i) = Lazy.optimistic(() => f(index, v.get))
      i += 1
    }
    
    result
  }
  
  def eagerMap(f : (Int,T) => T) : LazyArray[T] = {
    var newValues = values.clone()
    val result = new LazyArray(newValues)
    
    var i = 0
    while(i < values.length) {
      val index = i
      val v = values(i)
      if(v != null)
        newValues(i) = f(index, v.get)
      i += 1
    }
    
    result
  }
  
  def reduce[A](accum : A, f : (A,T) => A) = {
    var result = accum
    
    var i = 0
    while(i < values.length) {
      if(values(i) != null)
        result = f(result, values(i).get)
      i += 1
    }
    
    accum
  }
  
  def foreach(f : (Int,T) => Unit) = {
    var i = 0
    while(i < values.length) {
      if(values(i) != null)
        f(i, values(i).get)
      i += 1
    }
  }
  
  def get(index : Int) : T = values(index).get
  
  def forceAll : Unit = {
    var i = 0
    while(i < values.length) {
      if(values(i) != null)
        values(i) = Value(values(i).get)
      i += 1
    }
  }
}
