package SparkCore_02

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*数据格式: timestamp   province   city	userid	adid
            时间点 	     省份	     城市	 用户    广告
            1516609143867 6         7     64     16
用户ID范围:0-99
省份,城市,ID相同:0-9
adid:0-19
需求:
1.统计每一个省份点击TOP3的广告ID */
object Day_Task2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MyTool.createSparkContext("Day_Task2", "local[*]")
    //读取文件进行转换，提取出需要的字段  ((省份,广告id),1)
    val file: RDD[String] = sc.textFile("dir/SparkCore_day1/Advert.txt")
    val line: RDD[Array[String]] = file.map(_.split(" "))
    val temp: RDD[((String, String), Int)] = line.map(arr => ((arr(1), arr(4)), 1))
    //根据（省份,广告id)累加
    val sums: RDD[((String, String), Int)] = temp.reduceByKey(_ + _)
    //进行转换(省份,(广告id,累加))  然后分组
    val grouped: RDD[(String, Iterable[(String, Int)])] = sums.map(x => ((x._1._1), (x._1._2, x._2))).groupByKey()
    //将(广告id,累加)转为list 然后分组,求top3
    val top3: RDD[(String, List[(String, Int)])] = grouped.map(x => (x._1, x._2.toList.sortWith(_._2 > _._2).take(3)))
    top3.foreach(println)
    sc.stop()
  }
}
