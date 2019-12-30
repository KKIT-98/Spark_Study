package Kafka

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    //集群配置
    properties.put("bootstrap.servers","192.168.100.200:9092,192.168.100.201:9092,192.168.100.202:9092")
    //指定消费者组
    properties.put("group.id","group01")
    //3.指定消费位置
    // earliest 当各分区下有已提交offset,从提交的offset开始消费,无提交offset时,从头开始消费
    // latest  当各分区下有已提交offset,从提交的offset开始消费,无提交offset时,消费新产生的该分区下的数据
    // none  topic个分区存在已提交offset时,从offset后开始消费,只要有一个分区不存在提交offset,则抛出异常
    properties.put("auto.offset.reset","earliest")
    //4.需要对key和value进行反序列
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    //创建消费者进行消费   //key和value泛型和  配置文件参数
    val consumer = new KafkaConsumer[String, String](properties)
    //首先订阅Topic
    consumer.subscribe(Collections.singletonList("test"))
    //开始消费
    while (true){
      //参数是毫秒  consumer可能会拉取空数据，所以这个时长是等待时长,如果拉去到空数据,那么下次拉去等待时长就是参数
      val masg: ConsumerRecords[String, String] = consumer.poll(1000)
      val iter: util.Iterator[ConsumerRecord[String, String]] = masg.iterator()
      while (iter.hasNext){
        val msg: ConsumerRecord[String, String] = iter.next()
        println(s"partition:${msg.partition()},offset:${msg.offset()},key:${msg.key()},value:${msg.value()}")
        //ps:执行整个代码一定要开启Kafka集群,然后再在运行Producer和Consumer(一定是先生产在消费)
      }
    }
  }
}
