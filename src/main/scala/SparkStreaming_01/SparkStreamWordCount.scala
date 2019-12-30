package SparkStreaming_01
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreamWordCount {
  def main(args: Array[String]): Unit = {
    /*
  写SparkStreaming程序时候不能使用local作为本地模式
  原因在于SparkStreaming是一个长期执行任务,并且这个任务需要一个接受器来接受数据
  所以就需要使用最少需要2个核心,一个是给接收器使用,另外一个是用来计算使用
  设置Master的时候最好是使用 local[*] 或 local[2以上(含2的数值)]
  */
    //创建SparkStreaming对象,需要使用StreamingContext
    //所以SparkStreaming对象的创建和SparkCore的创建类似
    //1,配置SparkConf对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamWordCount").setMaster("local[*]")
    //2.创建Streaming对象
    //参数有两个值 一个是conf对象,另外一个是批次间隔(时间间隔)
    //Seconds 代表的是以秒为单位, 数值就是秒数
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //第二种
    /*val sc = new SparkContext(conf)
    val streamingContext1 = new StreamingContext(sc, Seconds(5))*/
    //获取实时数据, 从netcat服务器端获取数据
    //    //参数是hostname名称(相当于IP),port必须和netcat所监听的端口号一致
    val Dstream: ReceiverInputDStream[String] = streamingContext.socketTextStream("master", 6666)
    //RDD中如何处理整个数据,SparkStreaming的程序就如何来处理
    //    //RDD中大部分算子在DStream中都是通用的,而且写实时处理数据的逻辑和处理离线的逻辑也是类似
    //    //DStream中算子返回的是DStream
    val sumed: DStream[(String, Int)] = Dstream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //触发算子不一样DStream中不叫Action而是叫做output
    //打印到控制台
    sumed.print()
    //开启任务
    streamingContext.start()
    //等待任务，处理下一个批次任务
    streamingContext.awaitTermination()
  }
}
