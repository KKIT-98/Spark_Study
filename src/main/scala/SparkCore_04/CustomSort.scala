package SparkCore_04
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object CustomSort {
  def main(args: Array[String]): Unit = {
    //基础版本排序
    val conf: SparkConf = new SparkConf().setAppName("CustomSort").setMaster("local")
    val sc = new SparkContext(conf)
    val info: RDD[(String, Int, Int)] = sc.parallelize(List(("xiaoming", 90, 32), ("laoMin", 70, 43), ("liHua", 50, 33),("lisi",31,50)))
    //对存储在元组中的颜值进行排序，也就是名字后面的"
    val value: RDD[(String, Int, Int)] = info.sortBy(_._2,false)
    println(value.collect.toBuffer)
    value.foreach(println)
    println("自定义排序1")
    //需要类存储数据并进行传递，这个类必须实现了序列号的接口
    import  MyOrdering.Teacherording
    val value1: RDD[(String, Int, Int)] = info.sortBy(t => Teacher(t._2, t._3))
    println(value1.collect.toBuffer)
    sc.stop()
  }
}
