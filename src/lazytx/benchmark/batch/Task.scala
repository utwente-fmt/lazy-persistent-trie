package lazytx.benchmark.batch

import lazytx.Diff

abstract class Task[T >: Null, D <: Diff[T]] {
  def enqueue(diff : D) : Unit
  def result(state : T) : Unit
}