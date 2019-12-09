package SparkCore_01
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    //调用一个算子
    val line: RDD[String] = sc.textFile("dir/SparkCore_day1/file.txt")
    val words: RDD[String] = line.flatMap(_.split(" "))
    val tuples: RDD[(String, Int)] = words.map((_, 1))
    //Spark提供了一个根据key计算value的值（这个算子是使用最广泛的算子），相同key为一组计算一次value的值
    val sum: RDD[(String, Int)] = tuples.reduceByKey(_ + _)
    println(sum.collect().toList)
    sc.stop()
  }
}
