package SparkCore_02

import org.apache.spark.rdd.RDD

//创建RDD方式一:使用集合创建RDD
object CreateRdd_1 {
  def main(args: Array[String]): Unit = {
    val sc = MyTool.createSparkContext("CreateRdd_1", "local")
    //使用makeRDD进行创建
    //第一个参数是 存储的集合  第二个参数是分区数量,默认是是根据local决定,也可以传入一个具体值
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 45))
    rdd1.foreach(println)
    //使用makeRDD底层进行创建
    val rdd2: RDD[Int] = sc.parallelize(Array(2, 34, 5, 6, 7, 9, 8))
    rdd2.foreach(println)
    sc.stop()
  }
}
