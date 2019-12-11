package SparkCore_02
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*数据格式: timestamp   province   city	userid	adid
            时间点 	     省份	     城市	 用户    广告
用户ID范围:0-99
省份,城市,ID相同:0-9
adid:0-19
需求:
1.统计每一个省份点击TOP3的广告ID */
object Day_Task {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Day_Task")
    val sc: SparkContext = new SparkContext(conf)
    //文件中的每一行
    val lines: RDD[String] = sc.textFile("dir/SparkCore_day1/Advert.txt")
    val fun = (line:String) =>  ((line.split(" ")(1), line.split(" ")(4)), 1)
    //((省份,广告),计数）
    val provinceRDD: RDD[((String, String), Int)] = lines.map(fun)
    /*    val provinceRDD = lines.map(line => {
          val fileds = line.split(" ")
          ((fileds(1), fileds(4)), 1)
        })
        */
  //  provinceRDD.foreach(println)
   /*被包在花括号内没有match的一组case语句是一个偏函数，
    它是PartialFunction[A, B]的一个实例，A代 表参数类型，B代表返回类型，
    常用作输入模式匹配
    一个是apply()方法，直接调用可以通过函数体内的case进行匹配，返回结果;
    另一个是isDeﬁnedAt()方法，可以返回一个输入，是否跟任何一个case语句匹配
    */
    val rdd1: RDD[((String, String), Int)] = provinceRDD.reduceByKey(_ + _)

    val value1: RDD[(String, Iterable[(String, Int)])] = rdd1.map {
      case ((province, id), count) => (province, (id, count))
    } //(省份,(广告,计数))
    .groupByKey()
    value1.mapValues(x=> x.toList.sortBy(_._2).takeRight(3).reverse).foreach(println)
    sc.stop()
  }

}
