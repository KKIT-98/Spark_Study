package SparkCore_02

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CreateRdd_2 {
  def main(args: Array[String]): Unit = {
    //从外部数据直接创建RDD
    val sc: SparkContext = MyTool.createSparkContext("CreateRdd_2", "local")
    val file: RDD[String] = sc.textFile("dir/SparkCore_day1/file.txt")
    file.foreach(println)
    sc.stop()
  }
}
