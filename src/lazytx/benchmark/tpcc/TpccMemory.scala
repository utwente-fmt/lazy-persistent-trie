package lazytx.benchmark.tpcc

object TpccMemory {
  def main(args : Array[String]) : Unit = {
    val runtime = Runtime.getRuntime
    
    print("lazy ")
    for(warehouses <- Array(1, 2, 4, 8, 16, 32, 64, 128)) {
      System.gc
      val before = runtime.totalMemory() - runtime.freeMemory()
      val tpcc = LazyTpcc.generate(warehouses)
      System.gc
      val after = runtime.totalMemory() - runtime.freeMemory()
      
      tpcc.hashCode
      print((after - before) + " ")
    }
    println
    
    print("locking ")
    for(warehouses <- Array(1, 2, 4, 8, 16, 32, 64, 128)) {
      System.gc
      val before = runtime.totalMemory() - runtime.freeMemory()
      val tpcc = LockingTpcc.generate(warehouses)
      System.gc
      val after = runtime.totalMemory() - runtime.freeMemory()
      
      tpcc.hashCode
      print((after - before) + " ")
    }
    println
    
    print("stm ")
    for(warehouses <- Array(1, 2, 4, 8, 16, 32, 64, 128)) {
      System.gc
      val before = runtime.totalMemory() - runtime.freeMemory()
      val tpcc = StmTpcc.generate(warehouses)
      System.gc
      val after = runtime.totalMemory() - runtime.freeMemory()
      
      tpcc.hashCode
      print((after - before) + " ")
    }
    println
  }
}