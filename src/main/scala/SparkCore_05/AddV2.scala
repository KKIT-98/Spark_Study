package SparkCore_05
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
/*Spark2.x之后使用的全新累加器,如果在开发使用必须使用这个版本*/
object AddV2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AddV2").setMaster("local")
    val sc = new SparkContext(conf)
    //全新的累加器V2提供了2个系统计算累加器一个计算Long类型 另外一个计算的是Double
    val num1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9),2)
    val num2: RDD[Double] = sc.parallelize(List(1.1, 2.1, 3.3, 4.3, 3, 1, 1), 2)
    //==使用方法的形式完成累加器操作==
    //Long累加器
    //1.提供一个方法并注册累加器对象
    def longAccumulator(name:String):LongAccumulator={
      val acc = new LongAccumulator
      //2.对累加器进行注册，累计签到额名称可以自定义，通过SparkContext触发注册
      sc.register(acc,name)
      acc
    }
    val longaccs: LongAccumulator = longAccumulator("longadd")
    //这里需要调用一个方法 这个方法名是add方法的作用就是累加计算
    num1.foreach(x => longaccs.add(x))
    //获取累加器的值
    println(longaccs.value)
    //Double累加器
    def doubleAccumulatoe(name:String):DoubleAccumulator ={
    val acc = new DoubleAccumulator
    sc.register(acc,name)
    acc
    }
    val acc2: DoubleAccumulator = doubleAccumulatoe("doubleadd")
     num2.foreach(y => acc2.add(y))
    println(acc2.value)
    sc.stop()
  }
}
