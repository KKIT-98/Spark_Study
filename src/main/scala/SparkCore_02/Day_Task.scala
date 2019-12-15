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
    //1、加载数据
    val lines: RDD[String] = sc.textFile("dir/SparkCore_day1/Advert.txt")
    //一行数据进行切分
    val line: RDD[Array[String]] = lines.map(_.split(" "))
    //2、需求:每一个省份点击top3的广告:
    //找需要使用的字段：省份   广告    计数(需自定义)
    //获取对应的值就是省份和广告id，进行转换:省份_广告id ,1
    val sfandadid: RDD[(String, Int)] = line.map(arr => (arr(1) + "_" + arr(4), 1))
    //这个省份下广告ID一共出现多少次,意味着要累加计数 --> 使用reduceByKey
    val sum: RDD[(String, Int)] = sfandadid.reduceByKey(_ + _)//统计出每个省份广告所有的点击量
    //3、已将点击结果求出，现在需要根据省份进行计算。而现在数据没有根据省份聚合
    //没有相同省份下的数据，所以根据省份分组
    //转换-->   省份_广告id ,1  省份,(广告ID,点击量)
    val sums: RDD[(String, (String, Int))] = sum.map { tup =>
      val arr: Array[String] = tup._1.split("_")
      (arr(0), (arr(1), tup._2))
    }
    //需聚合省份数据:key(省份) value:(广告ID,点击量)
    val groupedsf: RDD[(String, Iterable[(String, Int)])] = sums.groupByKey()
    //对value进行操作可得这个省份的top3
    val top3: RDD[(String, List[(String, Int)])] = groupedsf.mapValues(v => v.toList.sortWith((x, y) => x._2 > y._2).take(3))
    top3.foreach(println)
    sc.stop()
  }

}
