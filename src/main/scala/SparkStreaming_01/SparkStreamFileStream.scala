package SparkStreaming_01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
//消费者策略   offset维护   zekkper在kafak中的作用   会写生产者  消费者  代码  自己写   sparkStream原理    Dstrm概念  第一个Stream程序
//shuffle流程  粪桶 分区  数仓建模
//core  sql  stream
object SparkStreamFileStream {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkStreamWordCount").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //监控文件夹(这个路径可以写本地也可以写HDFS集群)
    val filestream: DStream[String] = streamingContext.textFileStream("dir/files/")
    //对数据进行处理
    val sum: DStream[(String, Int)] = filestream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    sum.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
