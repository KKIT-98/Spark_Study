package SparkCore_03

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/*Action算子*/
/*action算子(行动算子) --> 是触发转换算子计算一个action算子触发,就会产生job
action算子的返回值基本上都不是RDD,所以在action算子后面在触发计算,就需要区分计算的数据了
*/
object ActionOperatorDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MyTool.createSparkContext("ActionOperatorDemo", "local[*]")
    //1、reduce :计算求和，多用于没有key的情况下进行求和
    val rdd: RDD[Int] = sc.parallelize(List(1, 1, 4, 5, 6, 5), 2)
    val rdd_1: Int = rdd.reduce(_ + _)
    println("原数据:"+rdd.collect.toBuffer)
    println("reduce求和:"+rdd_1)
    //运行结果:
    // 原数据:ArrayBuffer(1, 1, 4, 5, 6, 5)
    //reduce求和:22
    //2、toBuffer:将RDD中数据转换Scala中数组存储[不可变数组]
    val rdd_2: mutable.Buffer[Int] = rdd.collect().toBuffer
    println(rdd_2)
    //3、count:返回RDD元素个数
    val count: Long = rdd.count()
    println("RDD元素个数:"+count)
    //结果：RDD元素个数:6
    //4、top:取出对应数量值,默认降序,若输入0,返回一个空数组[数组是一个不可变数据]
    val top3: Array[Int] = rdd.top(3)
    println("Top3"+top3.toList)
    //结果:Top3List(6, 5, 5)
    //5、take:顺序取出对应数量值 返回也是一个数组[是一个不可变数组]
    val take2: Array[Int] = rdd.take(2)
    println("顺序取出2个值:"+take2.toList)
    //结果:顺序取出2个值:List(1, 1)
    //6、takeOrdered:顺序取出对应数量的值, 默认是升序 返回的是一个数组[并不可变数组]
    val little2: Array[Int] = rdd.takeOrdered(2)
    println("升序取出前2个值:"+little2.toList)
    //结果:升序取出前2个值:List(1, 1)
    //7、first:取出第一个值 等价于  take(1)  取值等价,不等价返回值
    val fistvalue: Int = rdd.first() //注:返回值是int 和take(1)不一样
    println("第一个值:"+fistvalue)
    //结果:升序取出前2个值:List(1, 1)
    //8、saveAsTextFile:对输出数据进行处理,将数据写成文件
    //rdd.saveAsTextFile("文件存储路径[本地/hdfs]")
    //9、countBykey: 统计key的个数并生成一个Map k是key的名字 v 是key的个数
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 2), ("c", 2), ("b", 1)))
    val rdd1_1: collection.Map[String, Long] = rdd1.countByKey()
    println(rdd1.collect().toBuffer)
    println(rdd1_1)
    //结果:
    // ArrayBuffer((a,1), (b,2), (c,2), (c,2), (b,1))
    //Map(a -> 1, b -> 2, c -> 2)
    //=============其他算子==================
    //countByValue:统计value的个数(但是会将集合中的一个元素看做是一个value并统计其个数)--> Action算子
    val countbyvalue: collection.Map[(String, Int), Long] = rdd1.countByValue()
    println(countbyvalue)
    //运行结果:Map((b,1) -> 1, (c,2) -> 2, (a,1) -> 1, (b,2) -> 1)
    // filterByRange:对RDD中元素进行过滤,但是指定范围内的数据(包括开始位置和结束位置)--> 转换算子

    val rangeFilter: RDD[(String, Int)] = rdd1.filterByRange("a", "b")
    println(rangeFilter.collect().toBuffer)
    //结果:ArrayBuffer((a,1), (b,2), (b,1))
    //flatMapValue:对应是kv对中的value值进行扁平化处理    --> 转换算子
    val rdd2: RDD[(String, String)] = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
    val rdd2_1: RDD[(String, String)] = rdd2.flatMapValues(_.split(" "))
    println(rdd2.collect().toList)
    println(rdd2_1.collect().toList)
    //结果:
    // List((a,1 2), (b,3 4))
    //List((a,1), (a,2), (b,3), (b,4
    //foreachPartition: (必记)循环处理分区内数据, 一般是用于数据持久化,即存入数据库中  --> Action
    val rdd3: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7), 2)
    rdd3.foreachPartition(x => println(x.reduce(_+_)))
    //结果:6 22    1 2 3一个分区   4 5 6 7一个分区
    //keyBy:以传入的函数作为key,RDD中元组作为value ,返回一个新的元组  --> 转换算子
    val value: RDD[(Int, (String, Int))] = rdd1.keyBy(_._1.length)
    println(value.collect().toBuffer)
    //ArrayBuffer((1,(a,1)), (1,(b,2)), (1,(c,2)), (1,(c,2)), (1,(b,1)))
    //collectAsMap:--> 将RDD中对偶元组(二元组) 转换为一个Map（必记） --> Action算子
    val map: collection.Map[String, Int] = rdd1.collectAsMap()
    println(rdd1.collect().toMap)
    println(map)
    //ArrayBuffer((a,1), (b,2), (c,2), (c,2), (b,1))
    //运行结果Map(b -> 1, a -> 1, c -> 2)
    //keys
    val keys: RDD[String] = rdd1.keys
    //value
    val values: RDD[Int] = rdd1.values
    println(rdd1.collect().toBuffer)
    println(keys.collect().toBuffer)
    println(values.collect().toBuffer)
    //运行结果:
    //ArrayBuffer((a,1), (b,2), (c,2), (c,2), (b,1))
    //ArrayBuffer(a, b, c, c, b)
   // ArrayBuffer(1, 2, 2, 2, 1)
  }
}
