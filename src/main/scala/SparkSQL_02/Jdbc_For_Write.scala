package SparkSQL_02

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*将数据写入数据库中*/
object Jdbc_For_Write {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Jdbc_For_Write")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据，如果不是json类型，使用RDD读然后转换为DataFrame数据类型
    val empy: DataFrame = spark.read.json("dir/SparkSQL/dataSource/employees.json")
    val p: DataFrame = spark.read.json("dir/SparkSQL/dataSource/people.json")
    //写出方式1
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","000000")
    //这个表可以不存在
    empy.write.jdbc("jdbc:mysql://localhost/sparkcore","empy1",properties)
    //写出方式2
    val people: RDD[String] = sc.textFile("dir/SparkSQL/dataSource/people.txt")
    val peoples: RDD[People] = people.map(x => {
      val arr: Array[String] = x.split(",")
      People(arr(0), arr(1).trim.toInt)
    })
    import spark.implicits._
    peoples.toDF().write.format("jdbc")
        .option("url","jdbc:mysql://localhost/sparkcore")
        .option("dbtable","people")
        .option("user","root")
        .option("password","000000")
        .save()
    //方式3:列需要和数据对应，列数据类型不能大写
    p.write.option("createTableColumnTypes","name varchar(20)").option("createTableColumnTypes","age bigint")
      .jdbc("jdbc:mysql://localhost/sparkcore","em",properties)
    spark.stop()
  }
}
case class People(Name:String,Age:Int)