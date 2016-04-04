package lazytx.benchmark.oltp

trait SimpleAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) : () => Unit
}

trait RandomAccessBenchmark extends SimpleAccessBenchmark {
  // Transactions should access data uniform randomly
}

trait SequentialAccessBenchmark extends SimpleAccessBenchmark {
  // Threads access data sequentially in one block
}

trait OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) : () => Unit
}