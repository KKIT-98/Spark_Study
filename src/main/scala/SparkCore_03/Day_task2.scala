package SparkCore_03

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*/*
用户点击产生日志信息,
有时间戳和对应URL,URL中会有学科的名称,统计学科的访问量
需求:根据用访问数据进行统计用户对各个学科的各个模块的访问量Top3
*/
//20161123101523 http://android.learn.com/android/video.shtml
//20161123101523 http://h5.learn.com/h5/teacher.shtml*/
object Day_task2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MyTool.createSparkContext("Day_task2", "local[*]")
    //读取文件
    val file: RDD[String] = sc.textFile("dir/SparkCore_day3/")
    //先按空格切，需要的数据是url
    val url: RDD[Array[String]] = file.map(_.split(" "))
    val urls: RDD[String] = url.map(arr => arr(1))
    //继续切分，取出需要的数据学科 模块 组成:((学科,模块),1)
    val temp: RDD[((String, String), Int)] = urls.map(aa => {
      val arrs: Array[String] = aa.split("/")
      ((arrs(3), arrs(4)), 1)
    })
    //按key累加计算
    val sums: RDD[((String, String), Int)] = temp.reduceByKey(_ + _)
    //转换 (学科,(模块,计数))
    val zh: RDD[(String, (String, Int))] = sums.map(x => ((x._1._1), (x._1._2, x._2)))
    //按key进行分组，
    val grouped: RDD[(String, Iterable[(String, Int)])] = zh.groupByKey()
    //将v转为list，排序(倒序)，
    val value: RDD[(String, List[(String, Int)])] = grouped.map(x => (x._1, x._2.toList))
    val res: RDD[(String, List[(String, Int)])] = value.map(x => (x._1, x._2.sortWith(_._2 > _._2)))//sortwith的下划线是代表list里面的数据
    res.foreach(println)
    sc.stop()
  }
}
