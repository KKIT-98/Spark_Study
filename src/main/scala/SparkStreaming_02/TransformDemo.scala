package SparkStreaming_02

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//Transform原语允许DStream上执行任意的RDD-to-RDD函数(直接操作存储在DStream内部RDD进行计算),
// 返回一个全新的DStream,这个DStream中是操作数据之后的结果
object TransformDemo {
  /*需求: 网站黑名单,某些IP禁止访问,广告点击量为1等等 屏蔽敏感字段*/
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TransformDemo").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    val getdata: ReceiverInputDStream[String] = streamingContext.socketTextStream("master", 6666)
    val tuple: DStream[(String, Int)] = getdata.flatMap(_.split(" ")).map((_, 1))
    //模拟黑名单
    val rdd: RDD[(String, Boolean)] = streamingContext.sparkContext.parallelize(List("ni", "wo", "ta")).map((_, true))
    //使用transform对DStream进行操作,获取里面的RDD进行处理
    val res: DStream[(String, Int)] = tuple.transform(rdd => {
      //对当前数据进行合并计算 将获取到的 单词kv 和 黑名单中kv进行合并使用的方式是 leftOuterJoin
      val leftrdd: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(rdd)
      //过滤
      val word: RDD[(String, (Int, Option[Int]))] = leftrdd.filter(x => {
        val a: (Int, Option[Int]) = x._2
        if (a._2.isEmpty) {
          true
        } else {
          false
        }
      })
      //返回最终过滤结果
      word.map(tup => (tup._1, 1))
    })
    val sumed: DStream[(String, Int)] = res.reduceByKey(_ + _)
    sumed.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
