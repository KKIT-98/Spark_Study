package SparkSQL_02

import org.apache.spark.sql.SparkSession

/*集群连接*/
object HiveOnSpark_Cluster {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("HiveOnSpark_Cluster")
      .master("spark://master:7077")//集群端的Spark
      .enableHiveSupport().getOrCreate()
      //对hive进行操作，默认操作是default
    /*session.sql(
      """
        |create table if not exists src_3 (
        |key int,
        |value String)
        |row format delimited
        |fields terminated by ' '
        |""".stripMargin)*/
    session.sql("show tables").show()
   // println("Success")
    session.stop()
  }
}
