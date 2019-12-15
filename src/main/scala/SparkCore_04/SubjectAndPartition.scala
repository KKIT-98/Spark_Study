package SparkCore_04

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*学科案例以及自定义分区*/
object SubjectAndPartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SubjectAndPartition").setMaster("local")
    val sc = new SparkContext(conf)
    val urls: RDD[(String, Int)] = sc.textFile("dir/SparkCore_day3/access.txt").map(x => {
      val url: Array[String] = x.split(" ")
      //取出url
      (url(1), 1)
    })
    val sunms: RDD[(String, Int)] = urls.reduceByKey(_ + _)
    val subandsum: RDD[(String, Int)] = sunms.map(x => {

      (new URL(x._1).getHost, x._2)
    })
    val grouped: RDD[(String, Iterable[(String, Int)])] = subandsum.groupBy(_._1)
    val toolist: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList)
    val ss: RDD[((String, List[(String, Int)]), Long)] = toolist.zipWithIndex()
    val zh: RDD[(Long, (String, List[(String, Int)]))] = ss.map(z => {
      (z._2, z._1)
    })
    val res: RDD[(Long, (String, List[(String, Int)]))] = zh.partitionBy(new CustomPartition(toolist.count().toInt))
    res.map(x =>{
      (x._2._1,x._2._2)
    })
   grouped.foreach(println)
    //println(toolist.collect().toBuffer)
   res.saveAsTextFile("./successs")

    sc.stop()
  }
}

class SubjectAndPartition(Numpart:Int) extends Partitioner{
  override def numPartitions: Int = Numpart

  override def getPartition(key: Any): Int = {
    key.toString.toInt % Numpart
  }
}
