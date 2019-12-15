/*作业要求:  将学科案例中根据不同学科的URL例如:android.learn.com,写到不同的文件中  即根据学科URL进行分区输出到不同文件中*/
package SparkCore_04
import org.apache.spark.Partitioner
import scala.collection.mutable
object SubJectAndPart {
  import java.net.URL
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{Partitioner, SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SubjectAndPartition").setMaster("local")
    val sc = new SparkContext(conf)
    val urls: RDD[(String, Int)] = sc.textFile("dir/SparkCore_day3/access.txt").map(x => {
      val url: Array[String] = x.split(" ")
      //取出url
      (url(1), 1)
    })
    val sunms: RDD[(String, Int)] = urls.reduceByKey(_ + _).cache()
    val subandsum: RDD[(String, (String, Int))] = sunms.map(x => {
      (new URL(x._1).getHost, (x._1, x._2))
    })
    //根据学科的URL(域名),进行数据输出(写成文件),不同学科写到不同文件中
    //问题一: 不知道有多少个学科,无法确定分区个数
    // 解决:  现在的元组 key是 学科  value 是一个元组对象(url,count)
    //         将当前学科获取出来,通过查看key发现key中有很多重复,此时将多余值去重,就可以得到剩余的学科
    //问题二: 通过查看已经知道学科个数,参数到自定义分区中时,是传递具体分区值还是传递key和分区值?
    //       如果传递分区值,无法获取key, 如果传递key和分区值 即可以得到key值还可以得到分区值所以值类Array
    //去重
    val keys: Array[String] = subandsum.keys.distinct().collect()
    //创建自定义分区对象
    val part: SubJectPart = new SubJectPart(keys)
    //重新分区
    val parted: RDD[(String, (String, Int))] = subandsum.partitionBy(part)
    //top3(遍历每个分区中的值进行计算)mapPartitions返回值是一个迭代器对象
    val res: RDD[(String, (String, Int))] = parted.mapPartitions(ite => {
      val list: List[(String, (String, Int))] = ite.toList
      //倒序
      val sorted: List[(String, (String, Int))] = list.sortBy(_._2._2).reverse
      sorted.iterator
    })
    res.saveAsTextFile("partout")
    sc.stop()
  }
}
/*自定义分区类的时候,构造方法中的参数一般是分区的个数
ps:除了Int类型做参数数据之外,还可以使用其他类型作为参数的数据类型来获取分区数
当前这个参数有key值和分区个数,但是传入自定义分区的参数是一个数组,并非是一个kv形似键值对*/
class SubJectPart(Numpart:Array[String]) extends Partitioner{
    //对Array这个数据进行处理形成KV:创建Map存储分区值和学科
    val map = new mutable.HashMap[String, Int]()
  //自定义分区逻辑，进行分区生成
    var i = 0 //计数器(也是分区ID)
    for (j <- Numpart){ //获取key值
      map += (j -> i)
      i += 1 //分区自增
    }
    override def numPartitions: Int = Numpart.size
    override def getPartition(key: Any): Int = map.getOrElse(key.toString,0)
  }

