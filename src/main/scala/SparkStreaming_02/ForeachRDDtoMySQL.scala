package SparkStreaming_02

import java.sql.{Connection, Driver, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/*将处理过的数据写到数据库*/
object ForeachRDDtoMySQL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ForeachRDDtoMySQL").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    val line: ReceiverInputDStream[String] = streamingContext.socketTextStream("master", 6666)
    streamingContext.checkpoint("checkpoint3")
    val tuple: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    val func = (word:String,value:Option[Int],status:State[Int]) => {
      val sum: Int = value.getOrElse(0) + status.getOption().getOrElse(0)
      val output: (String, Int) = (word, sum)
      status.update(sum)
      output
    }
    val sum: MapWithStateDStream[String, Int, Int, (String, Int)] = tuple.mapWithState(StateSpec.function(func))
    //将数据写入数据库中
    //第一种：基于partition连接，缺点: 这种方式虽然可以缓解外部数据压力,但是如果partition数据过多,也会导连接数量过多
    /*sum.foreachRDD(x => {
      x.foreachPartition(y =>{   //加载驱动
        Class.forName("com.mysql.jdbc.Driver")
        //获取mysql连接
        val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkcore?useSSL=false"
          , "root", "000000")
        //数据写入数据库
        for(i <- y){
          val sql = "insert into wordcount(titleName,count)values('"+i._1+"','"+i._2+"')"
          //执行插入
          connection.prepareStatement(sql).executeLargeUpdate()
        }
      })
    })*/
    //第二种:基于Lazy的静态连接缺点:这种方式一个Executor上的task都依赖于同一个连接度一项,所以会出现性能瓶颈(但是会解决多分区占用连接问题) 若需要一个完美的解决基于Lazy这个模式,使用静态连接池
    sum.foreachRDD(x => {
      x.foreachPartition(y =>{   //加载驱动
        Class.forName("com.mysql.jdbc.Driver")
        //获取mysql连接
        val connection: Connection = ClientConnect()
        //数据写入数据库
        for(i <- y){
          val sql = "insert into wordcount(titleName,count)values('"+i._1+"','"+i._2+"')"
          //执行插入
          connection.prepareStatement(sql).executeLargeUpdate()
        }
      })
    })
    sum.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
object ClientConnect{
  lazy val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkcore?useSSL=false","root","000000")
  def apply(): Connection = connection
}