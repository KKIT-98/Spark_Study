package SparkSQL_01
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/*RDD转换为DataFrame(直接转换)*/
object RDDToDataFrame {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    //获取数据
    val line: RDD[String] = sc.textFile("dir/SparkSQL/dataSource/people.txt")
    //将数据转换为元组存储---如果需要转换为DataFrame必须是元组类型
    val rddtup: RDD[(String, Int)] = line.map(x => {
      val arrs: Array[String] = x.split(",")
      (arrs(0), arrs(1).trim.toInt)
    })
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //将当前RDD转换为DataFrame  --> 直接转换--> 调用的是toDF,
    // 注:这个toDF不能直接使用,需要进行隐式导入,spark是自己创建的SparkSession'对象，不是系统的
    import spark.implicits._
    /*
      1.直接使用toDF方法,此时没有传入任何 参数列名默认就是元组中对应元素的取值名称及 _数字 缺点是不好操作
      2.调用toDF方法同时指定列的名字
     */

    rddtup.toDF("name","age").show()
    sc.stop()
  }
}
