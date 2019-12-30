package SparkSQL_02

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/*读取操作:读取数据库中的内容*/
object Jdbc_For_Read {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("Jdbc_For_Read").master("local").getOrCreate()
    //读取方式一
    //对连接数据库进行配置
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","000000")
    //读取内容
    val frame: DataFrame = session.read.jdbc("jdbc:mysql://localhost:3306/sparkcore", "location_info", properties)
    frame.show()
    //读取方式二
    val frame1: DataFrame = session.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkcore")
      .option("dbtable", "location_info")
      .option("user", "root")
      .option("password", "000000")
      .load()
    frame1.show()
    session.stop()
  }
}
