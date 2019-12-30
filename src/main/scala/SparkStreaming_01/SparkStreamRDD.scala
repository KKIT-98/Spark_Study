package SparkStreaming_01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable
object SparkStreamRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamRDD").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //创建rdd队列
    val rddqueue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    //读取队列:参数1:RDD队列对象    参数2:在该时间间隔内从队列仅仅使用1个RDD，默认使用true 建议使用false
    val rdds: InputDStream[Int] = streamingContext.queueStream(rddqueue, false)
    val sum: DStream[(Int, Int)] = rdds.map((_, 1)).reduceByKey(_ + _)
    sum.print()
    streamingContext.start()
    //循环创建RDD并存储到队列
    for (i <- 1 to 100){
      rddqueue += streamingContext.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }
    streamingContext.awaitTermination()
  }
}
