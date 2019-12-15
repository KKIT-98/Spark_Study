package SparkCore_05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*SequenceFile文件是Hadoop用来存二进制形式key-value对而设计
只能使用SequenceFile写出文件,在是哦用SequenceFile读取文件
使用SequenceFile数据写出的时候,它会帮你将数据类型自动转换为Writable类型
注:使用SequenceFile类型写出的文件人类不可读
*/
object SequenceFileIO {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SequenceFileIO").setMaster("local")
    val sc = new SparkContext(conf)
    //写出的数据是KV形式，写出什么格式就按什么格式读取
    val data: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
    data.saveAsSequenceFile("sequenceoutput")
    //对SequenceFile文件类型的文件进行读取
    //需要读取的数据类型，必须需要泛型
    val read: RDD[(String, Int)] = sc.sequenceFile[String, Int]("sequenceoutput")
    read.foreach(println)
    sc.stop()
  }
}
