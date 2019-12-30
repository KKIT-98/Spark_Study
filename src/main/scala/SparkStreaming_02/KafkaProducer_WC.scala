/*
package SparkStreaming_02
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random
object KafkaProducer_WC {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers","92.168.100.200:9092,192.168.100.201:9092,192.168.100.202:9092")
    //指定响应方式
    properties.put("acks","all")
    //请求失败重试次数
    properties.put("retries","3")
    //指定key的序列化方式，key用于存放数据对应的offset
    import org.apache.kafka.common.serialization.StringSerializer
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](properties)
    val array: Array[String] = Array("hello kity", "hello tom", "hello jerry", "hello scala")
    val random = new Random()
    while (true){
      val message: String = array(random.nextInt(array.length))
      producer.send(new ProducerRecord[String,String]("test1",message))
      println(message)
      Thread.sleep(1000)
    }
  }
}
*/
