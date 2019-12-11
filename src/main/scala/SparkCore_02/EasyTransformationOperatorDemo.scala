package SparkCore_02

import org.apache.spark.rdd.RDD

/*这里是一些简单的算子实例*/
object EasyTransformationOperatorDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkContext对象
    val sc = MyTool.createSparkContext("EasyOperatorDemo","local")
    //1、使用 map对RDD中每一个元素进行遍历并加以计算,返回一个全新RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    println(rdd.collect().toBuffer)
    //PS:ps:查看RDD中结果并打印到控制台上,collect[可以将RDD转换为Scala中Array数组(数组是不可变)] Action算子
    println("---------------使用map算子将rdd中的元素放大2倍-------------")
    val rdd2: RDD[Int] = rdd.map(_ * 2)
    println(rdd2.collect().toBuffer)
    //2.filter对RDD中每一个元素执行Boolean类型表达式,结果为true的值存储到新的RDD中
    val rdd3: RDD[Int] = rdd.filter(_ > 2)
    println("---------------使用filter算子将rdd中的元素大于2的放入新RDD-------------")
    println(rdd3.collect().toBuffer)
    //3. flatMap对RDD中存在集合进行压平处理,将集合内部的数据取出存储到一个全新的RDD中
    val rdd4: RDD[String] = sc.parallelize(Array("a b c d", "e", "f d c"))
    val rdd5: RDD[String] = rdd4.flatMap(_.split(" "))
    println("---------------使用flatMap算子将rdd4中的元素进行压平处理-------------")
    println("rdd4:"+rdd4.collect().toBuffer)
    println(rdd5.collect().toBuffer)
    /*注:map和FlatMap之间区别?
         这两者都是遍历RDD中数据,并对数据进行数据操作,并且会的到一个全新RDD
         Map多用于计算或处理一些特殊数据类型,不能使用扁平化处理的数据类型
         flatMap不仅可以对数据遍历处理,而且可以将存在RDD中集合中数据进行处理并且存储到一个新的集合中
         所以两种的使用本质上没有区别,但flatMap比Map多出了对集合数据压平的作用
         ps:一般情况下在Spark开发中较多使用flatMap,但是 flatMap不能使用所有的场景,所以也会使用map来进行处理数据
       */
    //4、sample 随机抽样(抽样只能在一个范围内返回 ,但是范围会有一定的波动)
    val rdd6: RDD[Int] = sc.parallelize(1 to 15)
    //随机抽样算子参数说明：
    /*
    withReplacement: Boolean, 表示抽取出数据是否返回原有样例中  true这个值会被放回抽样中 false 不会放回
    fraction: Double: 抽样比例 即 抽取30%  写入值就是 0.3(本身Double就是一个不精确数据)
    seed: Long = Utils.random.nextLong 种子, 随机获取数据的方式 ,默认不传
    */
    println("---------------使用sample随机抽样算子对rdd6中的元素进行30%抽样，抽完不放回-------------")
    val rdd6_1: RDD[Int] = rdd6.sample(false, 0.3)
    println("rdd6"+rdd6.collect().toBuffer)
    println(rdd6_1.collect().toBuffer)
    //5、union :求两个rdd的并集
    val rdd7_1: RDD[Int] = sc.parallelize(List(5, 6, 7, 8))
    val rdd7_2: RDD[Int] = sc.parallelize(List(1, 2, 5, 6))
    val rdd7: RDD[Int] = rdd7_1 union rdd7_2
    println("---------------两个rdd的并集---------------")
    println( rdd7_1.collect.toBuffer)
    println( rdd7_2.collect.toBuffer)
    println( rdd7.collect.toBuffer)
    //6、intersection :求两个rdd的交集
    val rdd7_jiaoji: RDD[Int] = rdd7_1 intersection rdd7_2
    println("---------------两个rdd的交集----------------")
    println( rdd7_jiaoji.collect.toBuffer)
    //7、distinct :去重
    println("---------------去重----------------")
    val rdd8: RDD[Int] = sc.parallelize(List(1, 3, 4, 5, 6, 34, 3, 2, 3))
    println(rdd8.collect().toBuffer)
    println(rdd8.distinct().collect().toBuffer)
    //8、Join ：相同key被合并,没有相同的key被舍弃掉
    val rdd9_1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
    val rdd9_2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2), ("tom", 2), ("dog", 10)))
    val rdd9: RDD[(String, (Int, Int))] = rdd9_1 join rdd9_2
    println("---------------join----------------")
    println( rdd9_1.collect.toBuffer)
    println( rdd9_2.collect.toBuffer)
    println( rdd9.collect.toBuffer)
    //9、leftOuterJoin和rightouterJoin：无论是左连接还是右连接,除了基本值外 ,剩余值的数据类型是Option类型
    val left: RDD[(String, (Int, Option[Int]))] = rdd9_1 leftOuterJoin rdd9_2
    val right: RDD[(String, (Option[Int], Int))] = rdd9_1 rightOuterJoin rdd9_2
    println("---------------左连接右连接---------------")
    println("left:"+left.collect().toBuffer)
    println("right:"+right.collect().toBuffer)
    //11、cartesian:笛卡尔积
    val dcart: RDD[((String, Int), (String, Int))] = rdd9_1 cartesian rdd9_2
    println("---------------笛卡尔积-----------------")
    println(dcart.collect().toBuffer)
    //10、分组:groupBy   groupByKey  cogroup
    //groupBy ：根据传入的参数分组
    val rdd10: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2), ("tom", 2), ("dog", 10),("dog",22),("Tom",3)))
    val rdd10_1: RDD[(String, Iterable[(String, Int)])] = rdd10.groupBy(_._1)
    println("----------------分组（对传入参数进行分组，这里是按第一个参数）-----------------")
    println(rdd10.collect().toBuffer)
    println(rdd10_1.collect().toBuffer)
    //groupByKey:根据key进行分组 对kv形式使用除了指定分组之后分区数量之外, 还可以使用自定义分区器
    val groupbykey: RDD[(String, Iterable[Int])] = rdd10.groupByKey()
    println("---------------分组（根据key进行分组 ）-----------------")
    println(rdd10.collect().toBuffer)
    println(groupbykey.collect().toBuffer)
   //cogroup:根据key进行分组(分组必须是一个对偶元组)
   val cogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd9_1 cogroup rdd9_2
    println("---------------分组cogroup（根据key进行分组 ）-----------------")
    println( rdd9_1.collect.toBuffer)
    println( rdd9_2.collect.toBuffer)
    println( cogroup.collect.toBuffer)
    //关闭SparkContext对象
    sc.stop()
    /* 
    ps:当前方法和groupByKey都可以对数据进行分组,但是,groupByKey会将相同key的值(value)存储在一起(一个集合)
    cogroup  参数是另外一个要合并分组的RDD(必须是对偶元组),根据相同key进行额分组,但是value不会存在一个集合中
     */
  }
}
