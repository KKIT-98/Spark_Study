package SparkCore_05
/*广播变量*/
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BroadCastDemo").setMaster("local")
    val sc = new SparkContext(conf)
    //在本地Driver定一个变量
    val list = List("hello spark")
    //算子进行数据处理在Executor端执行
    val file: RDD[String] = sc.textFile("dir/SparkCore_day5/file")
    //在Executor需要获取Driver端本地
    val filterstr: RDD[String] = file.filter(list.contains(_))
    filterstr.foreach(println)
    //修改为广播变量模型
    //在本地定义变量
    val list1 = List("hello hadoop")
    //将本地变量封装到广播变量中
    val broad: Broadcast[List[String]] = sc.broadcast(list1)
    //算子进行数据处理在Executor端执行
    val line: RDD[String] = sc.textFile("dir/SparkCore_day5/file")
    //Executor需获取广播变量
    val fil: RDD[String] = line.filter(broad.value.contains(_))
    fil.foreach(println)
    sc.stop()
  }
}
