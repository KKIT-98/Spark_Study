package SparkSQL_02

import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/*自定义UDAF函数支持DataFrame（注：UDAF函数属于聚合函数）*/
/*实现一个DataFrameUDAF函数，需创建一个class，需继承UserDefinedAggregateFunction并且实现内部的抽象方法*/
class MyAvg extends UserDefinedAggregateFunction {
  //输入数据Schema的信息
  // 需处理empno.json中的里面的薪水列
  override def inputSchema: StructType = StructType(List(
    StructField("salary",DoubleType,true)  //true代表可以为空
  ))
  //每个分区的共享变量，需要在分区计算中使用的属性
  //这里提供的属性就是分区聚合之后得到的结果集存储的位置
  override def bufferSchema: StructType = StructType(List(
    StructField("sum",DoubleType,true), //工资的总和
    StructField("count",DoubleType,true) //分区内工资累加次数
  ))
  //返回值的数据类型，表示UDAF函数最终输出结果的数据类型
  override def dataType: DataType = DoubleType
  //如果有相同输入，那么是否UDAF函数有相同输出，true就是相同输出、false不是(如果涉及到时间设置为false,不涉及时间设置为true)
  override def deterministic: Boolean = true
  //对buffer中的属性进行初始化操作，对每个共享变量进行赋值操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //工资总和赋值
    buffer(0) = 0.0 //取sum并赋值为0
    //工资次数
    buffer(1) = 0.0 //取count并赋值0
  }
  //对分区内的数据进行聚合操作
  //参数1:buffer，是共享变量(sum|count)  参数2:获取数据集读取一行的数据 {"name":"Minchael","salary":3000}
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //计算一行中工资的值
    buffer(0) = buffer.getDouble(0) + input.getDouble(0) //0.0+3000
    //工资聚合次数+1 统计次数
    buffer(1) = buffer.getDouble(1) + 1 // 0.0 + 1
  }
  //全局聚合、将分区中数据进行全局聚合
  //buffer1:存储全局聚合的值  buffer1(0):工资总和   buffer1(1):统计个数
  //buffer2:对应分区计算的结果值，，最终分区计算出的结果依旧是一个表结构，数据看作是Row对象
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //全局工资总和
    //buffer1.getDouble(0)相当于获取的是初始值 0.0 工资
    //buffer2.getDouble(0)相当于获取分区中计算的工资总和
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    //全局统计次数
    //buffer1.getDouble(0)相当于获取的是初始值 0.0 次数
    //buffer2.getDouble(0)相当于获取分区中计算的工资统计次数
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }
  //计算最终结果值。buffer就是全局聚合
  override def evaluate(buffer: Row): Double = buffer.getDouble(0) / buffer.getDouble(1)
}
object SparkSQL_UDAF_On_DataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkSQL_UDAF_On_DataFrame").master("local").getOrCreate()
    //读取数据
    val frame: DataFrame = spark.read.json("dir/SparkSQL/dataSource/employees.json")
    //注册函数
    spark.udf.register("MyAvg",new MyAvg)
    //创建临时表
    frame.createOrReplaceTempView("empno")
    //计算
    spark.sql("select MyAvg(salary) from empno").show()
    spark.stop()
  }
}
