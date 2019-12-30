package SparkStreaming_02

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*word count有状态转换*/
object UpdateStateByKey_WC {
  def main(args: Array[String]): Unit = {
    //使用无状态转换出现的问题：每次通过服务器端输入的数据,代码都可以正确的计算出结果,但是批次数据和批次数据之间并没有结果累加
    //虽然updateStateBykey可以计算历史数据和当前批次数据累加,但是必须提提供将历史批次数据存储方法。使用方式就是 checkpoint
    val conf: SparkConf = new SparkConf().setAppName("UpdateStateByKey_WC").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    //1.需要检查点,用来存储历史批次数据，有两种：本地（多用于测试），存储在HDFS集群(开发中必须使用)，在这里使用本地
    streamingContext.checkpoint("checkpoint1")
    //读取数据
    val Dstream: ReceiverInputDStream[String] = streamingContext.socketTextStream("master", 6666)
    //进行处理
    val tuple: DStream[(String, Int)] = Dstream.flatMap(_.split(" ")).map((_, 1))
    //第一种传递:指定计算逻辑
    //这个函数定义是为了更新当前状态历史的批次数据 + 当前批次数据
    //参数1类型:Seq[V] V是当前value中数据类型,ps:相当于是向前批次中相同key 对应vlaues的值 seq(1,1,1,1)
    //           (hello,1)(hello,1)(hello,1)(hello,1) --> 一个批次
    //参数1:values:Seq[Int]  key--> hello  values(1,1,1,1) ,当前批次中相同key对应的value值
    //参数2类型:Option[V] --> Option可选类型 有两个子类 Some(有值)  None(没有值)
    //                   --> 主要对应的是历史批次中数据 泛型V是历史批次数据的数据类型
    //参数2:state 相当于是获取同一个批次相同key的累加结果
    //ps: 例如 当前批次中计算是hello这个key state就会去查找历史批次中是否存在相同key.如果有就取出 Some封装好的数据  如果没有就返回None
    //当前这个计算逻辑最终返回值必须是一个Option类型
    val func1=(value:Seq[Int],state:Option[Int]) =>{
        //当前批次计算
        val dq: Int = value.sum
        //历史批次计算
        val history: Int = state.getOrElse(0) // getOrElse:有历史批次数据正常获取,如果没有返回0进行计算不会影响结果
        Some(dq+history)
    }
    val sum1: DStream[(String, Int)] = tuple.updateStateByKey(func1)
    //计算逻辑2
    // 以一个迭代器对象形式来影响计算结果 =>(Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
    // 第一个参数数K 是单词key的数据类型
    //    Seq[V] V是value的数据类型 --> 相同key对应所有value的一个集合
    //    Option[S] S是历史数据中 数据类型 --> 获取历史批次数据的值
    //    返回值必须是一个迭代器对象, 参数是最新计算key和value
    val func2 = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
      iter.map(t => {
        (t._1,t._2.sum+t._3.getOrElse(0))
      })
    }
    //参数说明:
    //第一个参数是计算逻辑
    //第二个参数是 自定义分区数 (HashPartitoner  或 自定义分区)
    //第三个参数是 表示是否在接下来SparkStreaming执行过程中产生相同RDD使用相同分区算法
    //ps:可以修改分区
    val sum2: DStream[(String, Int)] = tuple.updateStateByKey(func2, new HashPartitioner(streamingContext.sparkContext.defaultParallelism), true)
      sum2.print()
      streamingContext.start()
      streamingContext.awaitTermination()
  }
}
