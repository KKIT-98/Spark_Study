package SparkSQL_01
import org.apache.spark.sql.{DataFrame, SparkSession}
//使用SQL语法操作数据
object SparkSQL_SQL_Style {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSQL_SQL_Style").master("local").getOrCreate()
    //读取数据
    val people: DataFrame = sparkSession.read.json("dir/SparkSQL/dataSource/people.json")
    people.show()
    //当前的frame是不能使用sql语言进行操作,原因在于frame类似于RDD这样数据集,不能写SQL或HQL
    //需要使用HQL语言操作的化需要开启hive支持 enableHiveSupport
    //使用SQL语句进行操作
    //可以将当前frame注册成一张临时表或全局表
    //ps:建议使用临时而非全局
    //---->使用临时表<-----
    //临时表的范围就是在session范围之内有效,只要session退出,表就失效了
    //一个SparkSession结束之后,表自动删除
    //需要通过:DataFrame对象.createOrReplaceTempView("临时名")
    people.createOrReplaceTempView("People")
    //此时就可以使用SQL语言对当前表进行操作了
    sparkSession.sql("select * from People").show()
    //---->使用全局表<----
    //这个表可以作用于整个应用的内部范围
    //可以支持多个SparkSession对象对这个表进行访问 ,直到Session退出,这个表自动删除
    people.createGlobalTempView("people")
    //因为使用是全局表,所以在访问表名时候一定要注意路径问题即 访问表名时必须添加 global_temp.表名
    sparkSession.sql("select * from global_temp.people").show()
    sparkSession.stop()
  }
}
