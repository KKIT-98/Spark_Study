/*第一个Spark程序*/
package SparkCore_01
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SparkWordCount {
  //Spark程序都需要main
  def main(args: Array[String]): Unit = {
    //第一步:构建系统环境变量,为了SparkContext在环境变量所使用
    /*三个核心方法*/
    //1.set(key,value) --> 主要应对的是 环境变量设置  key 环境变量名  value 是具体值
    //2.setAppName(name) --> 设置程序运行的名称
    //3.setMaster(执行方式),如果需要运行本地环境,那么就需要配置SetMaster这个值
    //local:  --> 代表本地模式,相当于启用一个线程来模拟Spark运行
    //local[数值]:--> 代表本地模式, 根据数值来决定启用多少个线程来模拟spark运行
    //ps:数值不能大于当前cpu 核心数
    //"local[*]"  --> 代表本地模式 * 相当于是系统空闲多少线程就用多少线程来执行spark程序

    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
   // 第二步:构建SparkContext对象,需要对SparkContext对象进行环境配置即将conf对象传入到SparkContext中
    val sc: SparkContext = new SparkContext(conf)
    //第三步:Spark对数据处理
    //调用一个算子:读取文件内容,参数是文件路径(多用于读取txt或log文件)
    //val line: RDD[String] = sc.textFile("dir/SparkCore_day1/file.txt")
    //集群运行
    val line: RDD[String] = sc.textFile(args(0))
    //对文件中数据进行切分处理
    val words: RDD[String] = line.flatMap(_.split(" "))
    //对单词进行统计之前,需要对单词的个数进行计数
    val tuples: RDD[(String, Int)] = words.map((_, 1))
    //Spark提供了一个根据key计算value的值的算子（这个算子是使用最广泛的算子），相同key为一组计算一次value的值
    val sum: RDD[(String, Int)] = tuples.reduceByKey(_ + _)
    //println(sum.collect().toList)
    sum.saveAsTextFile(args(1))
    //关闭Sparkontext
    sc.stop()
    /*注：提交到集群运行需修改:
    sc.textFile(args(0)) //获取外部输入读取数据路径
    将数据文件存储到集群(也可以存储在本地)没有返回值
    sum.saveAsTextFile(args(1)) // 获取外部输入的存储路径 ,不打印语句*/
  }
}
