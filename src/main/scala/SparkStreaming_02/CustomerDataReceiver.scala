package SparkStreaming_02
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
object CustomerData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomerData").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //创建自定义receiver
    val line: ReceiverInputDStream[String] = streamingContext.receiverStream(new CustomerDataReceiver("master", 6666))
    //执行计算
    val sum: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //打印到控制台
    sum.print()
    //开启任务
    streamingContext.start()
    //等待下次任务
    streamingContext.awaitTermination()
  }
}
class CustomerDataReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
//在最初启动时候,调用该方法, 作为读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver"){
      //必须实现run方法重写(指定线程执行逻辑)
      override def run()={
          receive()
      }
    }.start()
  }
  //提供一个方法:读取数据并将数据发送给spark
  def receive()={
    //创建一个Socket对象,参数:主机名,端口号
    val socket = new Socket(host, port)
    //如果不是socket  直接  new reader
    //通过 BufferedReader流 来读取端口中数据StandardCharsets直接调用编码集
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
    //读取一行数据
    var input: String = reader.readLine()
    // 当receiver没有关闭并且输入数据部空,则循环发送数据给Spark
    while (!isStopped() && input != null){
      //接受数据进行保存
      store(input)
      input = reader.readLine()
    }
    //跳出循环则关闭资源
    reader.close()
    socket.close()
    //重新连接执行onStart,参数是一个字符串,随便写一个就可以
    restart("重新开始")
  }
  //程序停止的时候才执行
  override def onStop(): Unit = {}
}
