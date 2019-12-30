package SparkSQL_02

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/*自定义UDF函数*/
object SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSQL_UDF").master("local").getOrCreate()
    //读取数据
    val frame: DataFrame = sparkSession.read.json("dir/SparkSQL/dataSource/people.json")
    //编写UDF函数
    //注册函数可在整个应用中使用
    //参数1：函数名   参数2:函数实现
    sparkSession.udf.register("add", (x: String) =>  x+"-->test")
    //创建临时表
    frame.createOrReplaceTempView("people")
    sparkSession.sql("select add(name),age from people").show()
    sparkSession.stop()
  }
}
