package SparkStreaming_02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

//mapWithState只返回变换后Key的值,这样做的好处在于,
// 可以只关心那些已经发送变化Key,对于没有数据的数据,则不会返回那些没有变化key的数据
object MapWithState_WC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UpdateWithState_WC").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //创建检查点
    streamingContext.checkpoint("checkpoint2")
    //获取数据进行处理
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("master", 6666)
    val tuple: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //自写函数参数说明:
    //  第一个参数 输入数据key的数据(代表的是key)
    //  第二个参数 输入数据value (只能代表一个次数据)
    //  ps: updateStateByKey  --> value --> values Seq[Int] --> (1,1,1,1)
    //  (hello,1) -> 只计算这个一个1  若有多个相同key
    //  (hello,1) --> 1+1 ->2
    //  只会计算当前批次中相同key 对应的数据值
    //  第三个参数 代表的是历史批次状态的数值 ,上一次累加的结果
    val fun = (word:String,value:Option[Int],state:State[Int]) =>{
          //累加当前批次数据和历史批次数据
          //state是一个状态数据(当前这个key的状态是什么)不能直接计算
          val sum: Int = value.getOrElse(0) + state.getOption().getOrElse(0)
          //当前是一个批次中一个key的数据,所以就需要对这个数据进行重新拼接并更新状态
          val output: (String, Int) = (word, sum)
          //更新状态
          state.update(sum)
          //返回当前KV类型
          output
    }
    //计算逻辑,参数必须使用其给定的调用方式及StateSpec.function(状态更新函数)
    val sums: MapWithStateDStream[String, Int, Int, (String, Int)] = tuple.mapWithState(StateSpec.function(fun))
    sums.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
