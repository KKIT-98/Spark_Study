package SparkStreaming_02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object StreamingWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StreamingWindowDemo").setMaster("local[*]")
    //milliseconds 设置的是毫秒值 , 1000毫秒 == 1秒
    val streamingContext = new StreamingContext(conf, Milliseconds(5000))
    val line: ReceiverInputDStream[String] = streamingContext.socketTextStream("master", 6666)
    val s: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1))
    //计算10秒内的数据, 窗口的滑动间隔是10秒
    val sum: DStream[(String, Int)] = s.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Milliseconds(10000), Milliseconds(10000))
    sum.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
