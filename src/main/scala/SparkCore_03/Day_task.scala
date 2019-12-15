package SparkCore_03

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
用户点击产生日志信息,
有时间戳和对应URL,URL中会有学科的名称,统计学科的访问量
需求:根据用访问数据进行统计用户对各个学科的各个模块的访问量Top3
*/
//20161123101523 http://android.learn.com/android/video.shtml
//20161123101523 http://h5.learn.com/h5/teacher.shtml
object Day_task {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MyTool.createSparkContext("Day_task", "local[*]")
    val file: RDD[String] = sc.textFile("dir/SparkCore_day3/access.txt")
    //数据:20161123101523 http://h5.learn.com/h5/teacher.shtml
    //想要:(模块,1)
    val lines: RDD[Array[String]] = file.map(_.split(" "))
    val url: RDD[String] = lines.map(arr => arr(1))
    val urls: RDD[Array[String]] = url.map(_.split("/"))
    //val value: RDD[(String, String, String, String, String)] = urls.map(a => (a(0), a(1), a(2), a(3), a(4)))
    //(http:,,java.learn.com,java,javaee.shtml
    //(http:,,ui.learn.com,ui,video.shtml)
    //(http:,,bigdata.learn.com,bigdata,teacher.shtml)
    //其中a(3)是学科    a(4)是模块
    //((学科,模块)，1)
    val rr: RDD[((String, String), Int)] = urls.map(a => ((a(3), a(4)), 1))
    //按(学科，模块)进行累计加
    val sums: RDD[((String, String), Int)] = rr.reduceByKey(_ + _)
    //转换为(学科(模块,1))
    val v: RDD[(String, (String, Int))] = sums.map(x => (x._1._1, (x._1._2, x._2)))
    //按key进行分组
    val grouep: RDD[(String, Iterable[(String, Int)])] = v.groupByKey()
    //将v转换为list
    val tolist: RDD[(String, List[(String, Int)])] = grouep.map(x => (x._1, x._2.toList))
    //按v的计数进行排序
    val sort: RDD[(String, List[(String, Int)])] = tolist.map(x => (x._1, x._2.sortWith(_._2 > _._2)))
    sort.foreach(println)

  }
}
