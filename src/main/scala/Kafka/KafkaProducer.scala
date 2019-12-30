package Kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //用来写配置
    val properties = new Properties()
    //对集群进行配置:
    ///1.指定请求的kafka 集群列表,/ps:尽量写主机名,不要写IP地址,
    properties.put("bootstrap.servers","192.168.100.200:9092,192.168.100.201:9092,192.168.100.202:9092")
    //2.指定响应方式(ack请求机制) -1和all 同一个概念
    properties.put("acks","all")
    //3.请求失败的重试次数
    properties.put("retries","3")
    //指定key 和value的序列化 key 和vlaue都是String类型
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    //创建生产者实例:需要两个泛型一个参数.泛型是key和value的数据类型 参数是配置文件
    val producer = new KafkaProducer[String, String](properties)

   //也可读取文件  buffle  readline    rdd   不使用
    //模拟一些数据发送给kafka
    for (i <- 1 to 100000){

      val msg = s"${i}:this is kafka data"
      //标准方式是 两个泛型  两个参数
      //泛型是key和value  参数是对应topic名称和数据
      producer.send(new ProducerRecord[String,String]("test",msg))
      Thread.sleep(500)
    }
    producer.close()

  }
}
