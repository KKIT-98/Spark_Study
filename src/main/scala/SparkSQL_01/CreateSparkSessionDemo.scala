package SparkSQL_01
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/*SparkSession三种创建方式*/
object CreateSparkSessionDemo {
  def main(args: Array[String]): Unit = {
      //方式1: SparkSession不需要使用new先构建builder
      //然后在设 appName(执行应用的名字) 或 master(运行方式) 或 config(设置当前可以添 加的属性)
      val SparkSession1: SparkSession = SparkSession.builder()
        .appName("CreateSparkSessionDemo")
        .master("local")
        .getOrCreate()
      //方式2: 先创建Config对象设置当前appName和master然后在将设置存储到 config里面
      val conf: SparkConf = new SparkConf().setAppName("CreateSparkSessionDemo").setMaster("local")
      val SparkSession2: SparkSession = SparkSession.builder().config(conf).getOrCreate()
      //方式3:.开启hive支持enableHiveSupport()  --> 如果想使用必须添加配置 一种是集群hive 另外一种是本地hive
      val SparkSession3: SparkSession = SparkSession.builder()
        .appName("CreateSparkSessionDemo")
        .master("local")
        .enableHiveSupport() //开启hive支持
        .getOrCreate()
      //关闭
      SparkSession1.stop()
      SparkSession2.stop()
      SparkSession3.stop()
   }
}
