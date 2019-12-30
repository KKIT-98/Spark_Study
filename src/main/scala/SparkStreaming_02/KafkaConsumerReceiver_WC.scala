/*
package SparkStreaming_02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

//使用0.8版本接口来完成kafka的对接
object KafkaConsumerReceiver_WC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaConsumerReceiver_WC").setMaster("local[*]")
    val contextStream: StreamingContext = new StreamingContext(conf, Seconds(5))
    //设置检查点
    contextStream.checkpoint("checkpoint4")
    //创建连接获取DStream对象
    val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      contextStream,
      "slave1:2181",
      "group01",
      Map("test" -> 1)//需要一个Map对象,来获取test数据 key topic名称 value 是topic分 区数量
    )
    //当前对象获取过来的是一个kv键值对，其中对key的操作无用，数据主要在value中，所以操作value
    val tups: DStream[(String, Int)] = kafkaDstream.flatMap(t => t._2.split(" ")).map((_, 1))
    val sum: DStream[(String, Int)] = tups.updateStateByKey((values: Seq[Int], sate: Option[Int]) => {
      Some(values.sum + sate.getOrElse(0))
    })
    sum.print()
    contextStream.start()
    contextStream.awaitTermination()
  }
}
*/
