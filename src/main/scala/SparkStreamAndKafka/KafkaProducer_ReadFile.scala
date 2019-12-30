package SparkStreamAndKafka
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.{BufferedSource, Source}
object KafkaProducer_ReadFile {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.put("bootstrap.servers","192.168.100.200:9092,192.168.100.201:9092,192.168.100.202:9092")
    properties.put("acks","all")
    properties.put("retries","3")
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    while (true){
      val source: BufferedSource = Source.fromFile("dir/SparkCore_day1/file.txt")
      val strings: Iterator[String] = source.getLines()
      while (strings.hasNext){
        val msg: String = strings.next()
        producer.send(new ProducerRecord[String,String]("readfile",msg))
        Thread.sleep(3000)
      }
      source.close()
      Thread.sleep(3000)
    }
    producer.close()
  }
}
