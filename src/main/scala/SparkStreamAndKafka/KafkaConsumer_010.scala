package SparkStreamAndKafka

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumer_010 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaConsumer_011").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    streamingContext.checkpoint("checkpoint5")
    val kafka: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      //指定key的反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      //指定value的反序列化
      "value.deserializer" -> classOf[StringDeserializer],
      //指定消费者组
      "group.id" -> "group01",
      //指定消费者方式:  earliest|latest|none
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: lang.Boolean)
    )
    val topic: Array[String] = Array("test1")
    val log: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      //消费者本地策略
      /*
      PreferBrokers-->仅仅在你spark的executor在相同的节点上,优先分配到存在Kafka broker的机器上 (就近原则)
      PreferConsistent-->大多数情况下使用, 一致性的方式分配到所有executor上.(主要就是为了均匀分配)
      PreferFixed(hostMap: collection.Map[TopicPartition, String])和PreferFixed(hostMap: ju.Map[TopicPartition, String])
      后面这里消费模式是一样,只不过参数不同,如果你的负载不均衡,可以通过这两种方式手动指定分配
      在没有指定map前提下,默认还是平均分配*/
      LocationStrategies.PreferConsistent,
      //执行消费者消费
      ConsumerStrategies.Subscribe(topic, kafka))
    //我们多Kakfa中的数据主要关心的是value值,所以只需要获取value就可以获取值  k是偏移量
    val lines: DStream[String] = log.map(_.value())
    val sum: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      Some(value.sum + state.getOrElse(0))
    })
    sum.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
