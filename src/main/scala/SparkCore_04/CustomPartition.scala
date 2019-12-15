package SparkCore_04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*自定义分区,构造器中参数是当前分区个数*/
object CustomPartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomPartition").setMaster("local")
    val sc = new SparkContext(conf)
    val value: RDD[(Int, Long)] = sc.parallelize(0 to 10).zipWithIndex()
    val fun = (index:Int,ite:Iterator[(Int,Long)]) =>{
      ite.map(t => "[PartID:"+index+"   Value:"+t+"]")
    }
    val res: Array[String] = value.mapPartitionsWithIndex(fun).collect()
    for (i <- res){
      println(i)
    }
   println("------自定义分区-----")
    val v2: RDD[(Int, Long)] = value.partitionBy(new CustomPartition(5))
    val v2s: Array[String] = v2.mapPartitionsWithIndex(fun).collect()
    for (i <-v2s ){
      println(i)
    }
    sc.stop()
  }
}
//自定义分区
class CustomPartition(numPart:Int) extends Partitioner{
  //获取分区个数
  override def numPartitions: Int = numPart

  override def getPartition(key: Any): Int = {
    //分区逻辑
    key.toString.toInt % numPart
  }
}
