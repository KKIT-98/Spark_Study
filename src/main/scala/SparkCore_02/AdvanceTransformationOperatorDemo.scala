package SparkCore_02
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
/*进阶算子演示*/
object AdvanceTransformationOperatorDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MyTool.createSparkContext("AdvanceTransformationOperatorDemo", "local[*]")
    //1、mapPartitions:遍历RDD获取RDD中每一个分区中元素的值并进行计算,然后返回一个新的RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3) //分三个区:(1,2)分区1  (3,4)分区2  (5,6)分区3
    //参数说明:
    //f: Iterator[T] => Iterator[U]:第一个参数:是一个迭代器对象,这个迭代器对象获取的是对应分区中的值
    //preservesPartitioning: Boolean = false:第二参数是否保留父RDD的Partition分区信息,会使用默认值(这个作用会关系宽窄依赖)
    val rdd1_1: RDD[Int] = rdd.mapPartitions(_.map(_ * 10)) //第一个下划线是迭代器对象 第二个下划线是迭代器对象中存储的值
    //注:如果RDD的数据量不大,而且存在分区,建议使用mapPartition算子代替map算子,可以加快对数据的处速度
    //   如果RDD中数量过于庞大例如10亿条,不建议使用mapPartition,因为出现oom
    println("-----------------------mapPartitions---------------------------")
    println(rdd.collect.toBuffer)
    println(rdd1_1.collect.toBuffer)
    //运行结果:
   // ArrayBuffer(1, 2, 3, 4, 5, 6)
   // ArrayBuffer(10, 20, 30, 40, 50, 60)
    //2、对RDD中每个分区中数据进行遍历操作(可以打印 或 可以计算)
    //写一个迭代方法或函数 执行迭代操作
    val Iter = (index:Int,ite:Iterator[(Int)]) => {
       ite.map(x => "PartID:"+index+" value:"+x)
   }
    //参数说明:
    // f: (Int, Iterator[T]) => Iterator[U], 第一个参数是操作当前分区数据的函数,元组汇总第一个参数是分区序号 第二个参数是分区数据封装到的迭代器对象
    // preservesPartitioning: Boolean = false 第二个参数是否保留父RDD的Partition分区信息,会使用默认值(这个作用会关系宽窄依赖)
    val rdd1_1_1: RDD[String] = rdd.mapPartitionsWithIndex(Iter)
    println("-----------------------mapPartitionsWithIndex---------------------------")
    println(rdd.collect.toBuffer)
    println(rdd1_1_1.collect.toBuffer)
    //运行结果:
    //ArrayBuffer(1, 2, 3, 4, 5, 6)
    //ArrayBuffer(PartID:0 value:1, PartID:0 value:2, PartID:1 value:3, PartID:1 value:4, PartID:2 value:5, PartID:2 value:6)
    //3、排序:
    // sortByKey :  根据key值进行排序,但是key必须实现Ordered接口,必须是一个对偶元组这个算子有第二参数,第二个参数是boolean值决定了是升序true还是降序false默认是true
    // sortBy:ortBy 与sortByKey类似,不同点在于sortBy可以根据传入的参数决定使用谁来排序
    //       第一个参数就是 根据谁排序(值)
    //       第二个参数就是 决定升序还是降序  是升序true  还是降序 false 默认是true
    val rdd2_1: RDD[(Int, String)] = sc.parallelize(Array((3, "a"), (2, "b"), (3, "c"), (5, "b")))
    val rdd2_2: RDD[Int] = sc.parallelize(List(1, 23, 44, 1, 0, 3, 431, 44, 112, 556, 2, 3, 67))
    val rdd2_key: RDD[(Int, String)] = rdd2_1.sortByKey()
    val rdd2_key1: RDD[(Int, String)] = rdd2_1.sortByKey(false)
    println("-----------------------SortByKey---------------------------")
    println(rdd2_1.collect.toBuffer)
    println("升序"+rdd2_key.collect.toBuffer)
    println("降序"+rdd2_key1.collect.toBuffer)
    //运行结果：
    //ArrayBuffer((3,a), (2,b), (3,c), (5,b))
    //升序ArrayBuffer((2,b), (3,a), (3,c), (5,b))
    //降序ArrayBuffer((5,b), (3,a), (3,c), (2,b))
    //sortby
    val groupby1: RDD[Int] = rdd2_2.sortBy(x => x, false)
    val groupby2: RDD[Int] = rdd2_2.sortBy(x => x, true)
    println("-----------------------sortBy---------------------------")
    println(rdd2_2.collect.toBuffer)
    println("升序"+groupby2.collect.toBuffer)
    println("降序"+groupby1.collect.toBuffer)
      //运行结果：
    //ArrayBuffer(1, 23, 44, 1, 0, 3, 431, 44, 112, 556, 2, 3, 67)
    //升序ArrayBuffer(0, 1, 1, 2, 3, 3, 23, 44, 44, 67, 112, 431, 556)
    //降序ArrayBuffer(556, 431, 112, 67, 44, 44, 23, 3, 3, 2, 1, 1, 0)
    //注:Scala中sortBy和Spark中sortBy区别
    /* Scala中sortBy和Spark中sortBy区别:
       Scala中sortBy是以方法的形式存在的,并且是作用在Array或List集合排序上,并且这个sortBy默认只能升序, 除非实现隐式转换或调用reverse方法才能实现降序,
       Spark中sortBy是算子,作用出发RDD中数据进行排序,默认是升序,可以通过该算子的第二参数来实现降序排序的方式*/
    //4、重新分区
    //注:一般在计算阶段(即转换阶段)轻易不会分区数据量,在触发Job时才会对分区进行一些修改(控制输出)
    //   如果要在计算计算修改分区,绝对不会再发生shuffle的位置修改的,而且不要频繁修改分区(只要修改分区就会触发shuffle)
    val rdd3: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 5, 6), 3)
    //partitions：算子触发RDD中分区存储到一个数组中,然后获取数据长度即当前RDD分区的个数
    println("-----------------------partitions---------------------------")
    println(rdd3.collect().toBuffer)
    println("初始分区数:"+rdd3.partitions.length)
    //运行结果:
    // ArrayBuffer(1, 2, 3, 4, 5, 5, 6)
    //初始分区数:3
    //更改分区:
    // repartition :可以更改分区的数量 --> 会发生shuffle
    println("-----------------------repartition---------------------------")
    val rddlitter: RDD[Int] = rdd3.repartition(1) //缩小分区
    val rddbig: RDD[Int] = rdd3.repartition(6)//扩大分区
    println(rdd3.collect().toBuffer)
    println("分区个数(缩小)"+rddlitter.partitions.length)
    println("分区个数(扩大)"+rddbig.partitions.length)
    //运行结果:
    //ArrayBuffer(1, 2, 3, 4, 5, 5, 6)
    //分区个数(缩小)1
    //分区个数(扩大)6
    // coalesce:允许发生将分区个数修改为小值,但是不允许将分区个数为大值
     //注:coalesce 算子默认shuffle是false 不会发生shuffle,所以不能修改为大的分区值
    //            一定要使用这个算子修改分区,建议可以开启shuffle但是没有repartition好用
    //             这个算子是repartition底层实现 默认开启shuffle
    val lit: RDD[Int] = rdd3.coalesce(1)//缩小分区
    val big: RDD[Int] = rdd3.coalesce(6)//缩小分区
    val big_1: RDD[Int] = rdd3.coalesce(6,true)//缩小分区
    println("-----------------------coalesce---------------------------------")
    println(rdd3.collect().toBuffer)
    println("分区个数(缩小):"+lit.partitions.length)
    println("分区个数(扩大,不开启shuffle):"+big.partitions.length)
    println("分区个数(扩大,开启shuffle):"+big_1.partitions.length)
    //运行结果:
//    ArrayBuffer(1, 2, 3, 4, 5, 5, 6)
//    分区个数(缩小):1
//    分区个数(扩大,不开启shuffle):3
//    分区个数(扩大,开启shuffle):6
    // partitionBy:个算子是需要传入自定分区器的,因为还没有学习自顶分区,所以这里可以使用一个万能分区 HashPartitioner
    val rdd7: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("e", 6)), 2)
    val rddbi: RDD[(String, Int)] = rdd7.partitionBy(new HashPartitioner(4))
    println("-----------------------partitionBy---------------------------")
    println(rdd7.collect().toBuffer)
    println("原始分区:"+rdd7.partitions.length)
    println("扩大分区:"+rddbi.partitions.length)
    //运行结果:
//    ArrayBuffer((a,1), (b,2), (c,3), (e,6))
//    原始分区:2
//    扩大分区:4
    // repartitionAndSortWithPartitions：是repartition的一个变种,这个算子相当于集合了分区和排序
    //   这个算子只能对 对偶元组使用 --> 对偶元组(二元组),会根据key的值进行排序,
    //   如果需要在repartition之后再进行排序,此时就可以使用这个算子代替，需传入自定义分区器
val raswp: RDD[(String, Int)] = rdd7.repartitionAndSortWithinPartitions(new HashPartitioner(1))
    println("-------------repartitionAndSortWithPartitions--------------------")
    println(rdd7.collect().toBuffer)
    println("原始分区:"+rdd7.partitions.length)
    println("缩小分区:"+raswp.partitions.length)
    //运行结果:
//    ArrayBuffer((a,1), (b,2), (c,3), (e,6))
//    原始分区:2
//    缩小分区:1
    //6、求和:注:Spark中是很少使用Scala中求和方法sum reduce fold
    //          Scala中的reduce方法在Spark中是action算子
    //  reduceByKey:根据相同的key为一组进行求和(必须要有key)，返回值是一个全新的RDD,并且这个RDD中存储的是元组, key是原来的key value值时key所有value值的和
    val rdd8: RDD[(String, Int)] = sc.parallelize(List(("tom", 12), ("jerry", 13), ("tom", 1), ("jeck", 33), ("jerry", 2), ("jeck", 10)))
    val sumrdd8: RDD[(String, Int)] = rdd8.reduceByKey(_ + _)
    println("-----------------------reduceByKey-------------------------------")
    println(rdd8.collect().toBuffer)
    println(sumrdd8.collect().toBuffer)
    //运行结果：
    //ArrayBuffer((tom,12), (jerry,13), (tom,1), (jeck,33), (jerry,2), (jeck,10))
   // ArrayBuffer((tom,13), (jeck,43), (jerry,15))
    //aggregateByKey:根据相同key 为一组计算value的值，在kv对的RDD中,按照key将value进行分区合并,合并完成后在进行全局合并
    //aggregateByKey(zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U)
    // zeroValue:初始值(默认值) seqOP:局部聚合(分区内)  combOp:全局聚合(聚合分区计算的值)
    //计算流程:
    //1.先做每个分区内数据的计算,zeroValue会参与到每个分区的计算
    //2.zeroValue会和seqOP的逻辑进行计算,将聚合的结果会传递到combOp这个计算逻辑中也是根据相同key一组进行全局聚合
    val rdd9: RDD[(String, Int)] = sc.parallelize(List(("cat", 2), ("cat", 10), ("pig", 6), ("dog", 9), ("pig", 2), ("pig", 20), ("dog", 10), ("cat", 3)), 2)
    println("-----------------------aggregateByKey---------------------------")
    def f[T](index:Int,ite:Iterator[(T)])={
      ite.map(x => "[PartID]:"+index+"[Value]"+x)
    }
    println(rdd9.mapPartitionsWithIndex(f).collect().toList)
    //分区情况:
    //[PartID]:0[Value](cat,2), [PartID]:0[Value](cat,10), [PartID]:0[Value](pig,6), [PartID]:0[Value](dog,9),
    //[PartID]:1[Value](pig,2), [PartID]:1[Value](pig,20), [PartID]:1[Value](dog,10), [PartID]:1[Value](cat,3))
    //分区计算完:1分区:cat 10    pig 6  dog 9
    //          2分区:pig 20    dog 10 cat 3
    val res: RDD[(String, Int)] = rdd9.aggregateByKey(0)(math.max(_, _), _ + _)
    println(res.collect().toList)
    //结果：List((dog,19), (pig,26), (cat,13))

    //combineByKey: 根据key进行求和计算看相同key为一组计算 分区值 ,然后在计算全局的值(分区计算值+分区计算值)注：这个算子不能使用下划线
    //参数说明:
    //reateCombiner: V => C,遍历分区中所有元素,因此每个元素的key要么是还没有遇到,要么就是已经遇到如果这个key是第一次出现,此时会调用createCombiner的函数来创建这各key的对应累加初始值
    //mergeValue: (C, V) => C,  如果这各key在当前分区中遇到多个(多个相同key),它会使用mergeValue这各函数通过之前的累加初始值和其他相同key的value进行求和计算  [分区求和]
    //mergeCombiners: (C, C) => C  由于每个分区独立的,因此对于同一个key可以有多个累加器(不用分区内),如果有两个获取多个都有key累加器就使用mergeCombiners函数进行分区结果聚合  [全局聚合]
    val value: RDD[(String, Int)] = rdd9.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
    println("-----------------------combineByKey---------------------------")
    println(rdd9.collect().toList)
    println(value.collect().toList)
    //运行结果:
//    List((cat,2), (cat,10), (pig,6), (dog,9), (pig,2), (pig,20), (dog,10), (cat,3))
//    List((dog,19), (pig,28), (cat,15))
    sc.stop()
  }
}
