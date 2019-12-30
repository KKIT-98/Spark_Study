package SparkSQL_02

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator


/*创建UDAF函数支持DataSet，创建一个class并继承Aggregator，重写方法*/
//因为是强类型，需对泛型进行辅助，该赋值建议使用case class(样例类)
case class Employee(name:String,salary:Double)
case class Average(var sum:Double,var count:Double)
class MyUDAF_onDataSet extends Aggregator [Employee,Average,Double] {
  //初始化方法:初始化每一个分区中共享变量即 定义在Average类中属性
  override def zero: Average = Average(0.0,0.0)
  //对分区内数据进行聚合
  override def reduce(b: Average, a: Employee): Average = {
    //分区工资聚合
    b.sum += a.salary
    //分区次数聚合
    b.count += 1
    //聚合结果返回
    b
  }
  //全局聚合
  override def merge(b1: Average, b2: Average): Average = {
    //将分区内工资全局聚合
    b1.sum += b2.sum //0.0 + 3000
    //将分区内统计次数全局聚合
    b1.count += b2.count //0 + 1
    b1
  }
  //最终计算结果聚合
  override def finish(reduction: Average): Double = reduction.sum / reduction.count
  //需要进行编码设置。当前样例类即 Average 的编码  可以将样例类转换Scala元组
  override def bufferEncoder: Encoder[Average] = Encoders.product
  ///设置最终输出结果的编码器//这个包千万不要导入错误,记住一个点始终操作的是SparkSQL
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
object SparkSQL_UDAF_On_DataSet {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkSQL_UDAF_On_DataFrame").master("local").getOrCreate()
    //读取数据 注意:需返回DataSet类型的数据
    import spark.implicits._
    val dataset: Dataset[Employee] = spark.read.json("dir/SparkSQL/dataSource/employees.json").as[Employee]
    val ss: TypedColumn[Employee, Double] = new MyUDAF_onDataSet().toColumn.name("avg_salary")
    dataset.select(ss).show()
    spark.stop()
  }
}
