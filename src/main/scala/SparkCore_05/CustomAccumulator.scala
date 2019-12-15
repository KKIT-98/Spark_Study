package SparkCore_05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/*自定义累加器*/
/*
  * 1.必须继承AccumulatorV2这个类,还需要提供泛型类型
  * 累加器V2提供两个泛型占位符
  * In --> 代表的是 触发累加器时计算数据的数据类型
  * OUT --> 代表的是 触发累加器时计算结果的输出类型是什么
 */
object CustomAccumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomAccumulator").setMaster("local")
    val sc = new SparkContext(conf)
    val num: RDD[Int] = sc.parallelize(List(1, 3, 4, 5, 6, 56), 2)
    //创建累加器对象
    val myacc = new CustomAccumulator
    //注册
    sc.register(myacc,"myacc")
    // 切记不要使用Transformation(转换算子),会出现无法更新数据的情况应该使用是Action算子
    num.foreach(x => myacc.add(x))
    println(myacc.value)
    sc.stop()
  }
}

class CustomAccumulator extends AccumulatorV2[Int,Int]{
  //创建输出值变量
  var sum:Int = _
  //以下是必须重写的方法
  //检查方法是否为空，多用于对输出变量赋值，（将输出变量设置为初始值）
  override def isZero: Boolean = sum == 0
 //copy一个全新的累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    //创建累加器对象
    val accumulator = new CustomAccumulator
    //需将当前累加器对象拷贝到新累加器对象，这个拷贝将原有累加器中的值拷贝到新累加器中，为了累加器中的数据进行迭代计算
    accumulator.sum = this.sum
    accumulator
  }
  //重置累加器。将累加器中数据清0
  override def reset(): Unit = sum == 0
  //对每个分区内的数据进行计算（求和）
  override def add(v: Int): Unit = {
    sum += v //v是每个分区中的每个数据，
  }
  //合并每个分区中的数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }
  //输出
  override def value: Int = sum
}
