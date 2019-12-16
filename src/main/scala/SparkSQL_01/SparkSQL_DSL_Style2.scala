package SparkSQL_01

import org.apache.spark.sql.{DataFrame, SparkSession}

/*DSL风格语法*/
object SparkSQL_DSL_Style2 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SparkSQL_DSL_Style").master("local").getOrCreate()
    //读取数据
    val frame: DataFrame = session.read.json("dir/SparkSQL/dataSource/people.json")
    //DSL风格：类似于RDD中调用算子触发
    frame.show() //-->这也是一种DSL风格
    //查询name列的信息
    frame.select("name").show()
    //允许使用$ 取值符号，需隐式导包
    import session.implicits._
    frame.select($"name").show()
    //过滤数据 <===> where条件
    frame.filter($"age" > 20).show()
    session.stop()
  }
}
