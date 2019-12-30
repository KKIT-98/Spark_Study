package SparkStreamAndKafka

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumer_ReadFile {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaConsumer_ReadFile").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    streamingContext.checkpoint("ReadFileCheckpoint")
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
    val topic: Array[String] = Array("readfile")
    val SSS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topic, kafka))
    val line: DStream[String] = SSS.map(_.value())
    val sumed: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      Some(value.sum + state.getOrElse(0))
    })
    sumed.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
