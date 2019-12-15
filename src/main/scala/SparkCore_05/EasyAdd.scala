package SparkCore_05

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, Accumulator, SparkConf, SparkContext}

/*简单累加器演示*/
object EasyAdd {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("EasyAdd").setMaster("local")
    val sc = new SparkContext(conf)
    val number: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    var sum = 0
    val sums: Unit = number.foreach(n => sum += n)
    println(sums)
    /*为什么当这个sums是0,因为foreach是没有返回值,整个计算过程都是在executor端完成
    打印sums的时候是在Driver端完成,实际中executor获取是sums这个变量副本修改也是executor端修改的
    所以Driver本地变量不会送到影响,所以打印的结果是0
     */
    //-->共享变量<---
    //使用累加器将当前sumed作为共享变量, 相当于Executor和Driver操作的都是用一个变量,一般Executor对变量完成
    //计算之后Driver也会受到影响,此时可以使用累加器形式来完成这个计算
    //当调用累加器的时候accumulator出现同一个黑色横线证明当前这个方法已经过时,不能再使用
    //在Spark2.x之后我们需要使用一个全新的累加器对象AccumulatorV2
    //在源码过时提示中出现了一句话 --> ("use AccumulatorV2", "2.0.0")
    val sumed: Accumulator[Int] = sc.accumulator(0)
    val res: Unit = number.foreach(m => sumed += m)
    println(sumed.value+""+res)
    sc.stop()
  }
}
