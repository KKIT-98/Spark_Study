
package SparkSQL_02

import org.apache.spark.sql.SparkSession

/*本地搭建*/
object HiveOnSpark_Local {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir","D:\\QianFenPeiXun\\Spark\\")  //因为使用本地，所以warehouse需本地磁盘存储
      .appName("HiveOnSpark_Local")
      .master("local[*]").enableHiveSupport().getOrCreate()
    //创建一张表
   // session.sql("create table if not exists src1(key int,value String)")
    //插入数据
    //session.sql("load data local inpath 'dir/SparkSQL/dataSource/kv1.txt' into table src1")
    session.sql("select * from src1").show()
    println("插入数据成功")
    session.stop()
  }
}
